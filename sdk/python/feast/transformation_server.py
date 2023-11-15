import logging
import sys
from concurrent import futures
import threading

import grpc
import pyarrow as pa
from grpc_reflection.v1alpha import reflection
from fastapi import FastAPI, Response, status
import gunicorn.app.base

from feast.errors import OnDemandFeatureViewNotFoundException
from feast.feature_store import FeatureStore
from feast.protos.feast.serving.TransformationService_pb2 import (
    DESCRIPTOR,
    TRANSFORMATION_SERVICE_TYPE_PYTHON,
    GetTransformationServiceInfoResponse,
    TransformFeaturesResponse,
    ValueType,
)
from feast.protos.feast.serving.TransformationService_pb2_grpc import (
    TransformationServiceServicer,
    add_TransformationServiceServicer_to_server,
)
from feast.version import get_version

log = logging.getLogger(__name__)

def get_health_check_app():
    app = FastAPI()

    @app.get("/health")
    def health():
        return Response(status_code=status.HTTP_200_OK)

    return app


class TransformationServer(TransformationServiceServicer):
    def __init__(self, fs: FeatureStore) -> None:
        super().__init__()
        self.fs = fs

    def GetTransformationServiceInfo(self, request, context):
        response = GetTransformationServiceInfoResponse(
            type=TRANSFORMATION_SERVICE_TYPE_PYTHON,
            transformation_service_type_details=f"Python: {sys.version}, Feast: {get_version()}",
        )
        return response

    def TransformFeatures(self, request, context):
        try:
            odfv = self.fs.get_on_demand_feature_view(
                request.on_demand_feature_view_name
            )
        except OnDemandFeatureViewNotFoundException:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            raise

        df = pa.ipc.open_file(request.transformation_input.arrow_value).read_pandas()

        result_df = odfv.get_transformed_features_df(df, True)
        result_arrow = pa.Table.from_pandas(result_df)
        sink = pa.BufferOutputStream()
        writer = pa.ipc.new_file(sink, result_arrow.schema)
        writer.write_table(result_arrow)
        writer.close()

        buf = sink.getvalue().to_pybytes()

        return TransformFeaturesResponse(
            transformation_output=ValueType(arrow_value=buf)
        )
class FeastTransformationServeApplication(gunicorn.app.base.BaseApplication):
    def __init__(self, **options):
        self._app = get_health_check_app()
        self._options = options
        super().__init__()

    def load_config(self):
        for key, value in self._options.items():
            if key.lower() in self.cfg.settings and value is not None:
                self.cfg.set(key.lower(), value)

        self.cfg.set("worker_class", "uvicorn.workers.UvicornWorker")

    def load(self):
        return self._app

def _start_server(server):
    server_thread = threading.Thread(target=_run_server, args=[server])
    server_thread.daemon = True
    server_thread.start()
    return server_thread

def _run_server(server):
    try:
        server.start()
        server.wait_for_termination()
    except Exception as e:
        print(e)

def start_server(store: FeatureStore, port: int):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    add_TransformationServiceServicer_to_server(TransformationServer(store), server)
    service_names_available_for_reflection = (
        DESCRIPTOR.services_by_name["TransformationService"].full_name,
        reflection.SERVICE_NAME,
    )
    reflection.enable_server_reflection(service_names_available_for_reflection, server)
    server.add_insecure_port(f"[::]:{port}")
    server_thread = _start_server(server)
    FeastTransformationServeApplication().run()
    server_thread.join()

