package transformation

import (
	"bytes"
	"context"
	"fmt"
	"google.golang.org/protobuf/types/known/timestamppb"
	"strings"

	"github.com/apache/arrow/go/v8/arrow"
	"github.com/apache/arrow/go/v8/arrow/array"
	"github.com/apache/arrow/go/v8/arrow/ipc"
	"github.com/apache/arrow/go/v8/arrow/memory"
	"github.com/feast-dev/feast/go/internal/feast/model"
	"github.com/feast-dev/feast/go/internal/feast/onlineserving"
	"github.com/feast-dev/feast/go/protos/feast/serving"
	prototypes "github.com/feast-dev/feast/go/protos/feast/types"
	"github.com/feast-dev/feast/go/types"
	"google.golang.org/grpc"
	"io"
)

type grpcTransformationService struct {
	endpoint string
	project  string
}

func (s *grpcTransformationService) GetTransformation(
	ctx context.Context,
	featureView *model.OnDemandFeatureView,
	requestData map[string]*prototypes.RepeatedValue,
	entityRows map[string]*prototypes.RepeatedValue,
	features []*onlineserving.FeatureVector,
	numRows int,
	fullFeatureNames bool,
) ([]*onlineserving.FeatureVector, error) {
	var err error
	arrowMemory := memory.NewGoAllocator()

	inputFields := make([]arrow.Field, 0)
	inputColumns := make([]arrow.Array, 0)
	for _, vector := range features {
		inputFields = append(inputFields, arrow.Field{Name: vector.Name, Type: vector.Values.DataType()})
		inputColumns = append(inputColumns, vector.Values)
	}

	for name, values := range requestData {
		arr, err := types.ProtoValuesToArrowArray(values.Val, arrowMemory, numRows)
		if err != nil {
			return nil, err
		}
		inputFields = append(inputFields, arrow.Field{Name: name, Type: arr.DataType()})
		inputColumns = append(inputColumns, arr)
	}

	for name, values := range entityRows {
		arr, err := types.ProtoValuesToArrowArray(values.Val, arrowMemory, numRows)
		if err != nil {
			return nil, err
		}
		inputFields = append(inputFields, arrow.Field{Name: name, Type: arr.DataType()})
		inputColumns = append(inputColumns, arr)
	}

	inputRecord := array.NewRecord(arrow.NewSchema(inputFields, nil), inputColumns, int64(numRows))
	defer inputRecord.Release()

	recordValueWriter := new(ByteSliceWriter)
	arrowWriter, err := ipc.NewFileWriter(recordValueWriter)
	if err != nil {
		return nil, err
	}
	err = arrowWriter.Write(inputRecord)
	if err != nil {
		return nil, err
	}
	arrowInput := serving.ValueType_ArrowValue{ArrowValue: recordValueWriter.buf}
	transformationInput := serving.ValueType{Value: &arrowInput}

	req := serving.TransformFeaturesRequest{
		OnDemandFeatureViewName: featureView.Base.Name,
		Project:                 s.project,
		TransformationInput:     &transformationInput,
	}

	opts := make([]grpc.DialOption, 0)
	opts = append(opts, grpc.WithDefaultCallOptions())

	conn, err := grpc.Dial(s.endpoint, opts...)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	client := serving.NewTransformationServiceClient(conn)

	res, err := client.TransformFeatures(ctx, &req)
	if err != nil {
		return nil, err
	}

	arrowBytes := res.TransformationOutput.GetArrowValue()
	return ExtractTransformationResponse(featureView, arrowBytes, numRows, fullFeatureNames)
}

func ExtractTransformationResponse(
	featureView *model.OnDemandFeatureView,
	arrowBytes []byte,
	numRows int,
	fullFeatureNames bool,
) ([]*onlineserving.FeatureVector, error) {
	reader := bytes.NewReader(arrowBytes)
	arrowMemory := memory.NewGoAllocator()
	arrowReader, err := ipc.NewFileReader(reader, ipc.WithAllocator(arrowMemory))
	if err != nil {
		return nil, err
	}

	outRecord, err := arrowReader.Record(numRows)
	if err != nil {
		return nil, err
	}
	result := make([]*onlineserving.FeatureVector, 0)
	for idx, field := range outRecord.Schema().Fields() {
		dropFeature := true

		if featureView.Base.Projection != nil {
			var featureName string
			if fullFeatureNames {
				featureName = strings.Split(field.Name, "__")[1]
			} else {
				featureName = field.Name
			}

			for _, feature := range featureView.Base.Projection.Features {
				if featureName == feature.Name {
					dropFeature = false
				}
			}
		} else {
			dropFeature = false
		}

		if dropFeature {
			continue
		}

		statuses := make([]serving.FieldStatus, numRows)
		timestamps := make([]*timestamppb.Timestamp, numRows)

		for idx := 0; idx < numRows; idx++ {
			statuses[idx] = serving.FieldStatus_PRESENT
			timestamps[idx] = timestamppb.Now()
		}

		result = append(result, &onlineserving.FeatureVector{
			Name:       field.Name,
			Values:     outRecord.Column(idx),
			Statuses:   statuses,
			Timestamps: timestamps,
		})
	}
	return result, nil
}

type ByteSliceWriter struct {
	buf    []byte
	offset int64
}

func (w *ByteSliceWriter) Write(p []byte) (n int, err error) {
	capacity := len(p)
	writeSlice := w.buf[w.offset:]
	if len(writeSlice) < capacity {
		w.buf = append(w.buf, make([]byte, capacity-len(writeSlice))...)
		writeSlice = w.buf[w.offset:]
	}
	copy(writeSlice, p)
	w.offset += int64(capacity)
	return capacity, nil
}

func (w *ByteSliceWriter) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		if w.offset != offset && (offset < 0 || offset > int64(len(w.buf))) {
			return 0, fmt.Errorf("invalid seek: new offset %d out of range [0 %d]", offset, len(w.buf))
		}
		w.offset = offset
		return offset, nil
	case io.SeekCurrent:
		newOffset := w.offset + offset
		if newOffset != offset && (newOffset < 0 || newOffset > int64(len(w.buf))) {
			return 0, fmt.Errorf("invalid seek: new offset %d out of range [0 %d]", offset, len(w.buf))
		}
		w.offset += offset
		return w.offset, nil
	case io.SeekEnd:
		newOffset := int64(len(w.buf)) + offset
		if newOffset != offset && (newOffset < 0 || newOffset > int64(len(w.buf))) {
			return 0, fmt.Errorf("invalid seek: new offset %d out of range [0 %d]", offset, len(w.buf))
		}
		w.offset = offset
		return w.offset, nil
	}
	return 0, fmt.Errorf("unsupported seek mode %d", whence)
}
