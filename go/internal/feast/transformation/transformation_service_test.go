package transformation

import (
  "testing"
  "fmt"

	"github.com/apache/arrow/go/v8/arrow"
	"github.com/apache/arrow/go/v8/arrow/array"
	"github.com/apache/arrow/go/v8/arrow/ipc"
	"github.com/apache/arrow/go/v8/arrow/memory"
)

func TestSendGrpcRequest(t *testing.T) {
  allocator := memory.NewGoAllocator()
  dummyColumn := array.NewInt64Builder(allocator)


	inputFields := make([]arrow.Field, 0)
	inputColumns := make([]arrow.Array, 0)
  for i := 0; i < 10; i++ {
    for j := 0; j < 10; j++ {
      dummyColumn.Append(int64(j))
    }
		inputFields = append(inputFields, arrow.Field{Name: fmt.Sprintf("Field %d", i), Type: &arrow.Int32Type{}})
		inputColumns = append(inputColumns, dummyColumn.NewArray())
	}

  inputSchema := arrow.NewSchema(inputFields, nil)
	inputRecord := array.NewRecord(inputSchema, inputColumns, int64(10))
	defer inputRecord.Release()

	recordValueWriter := new(ByteSliceWriter)
	arrowWriter, _ := ipc.NewFileWriter(recordValueWriter, ipc.WithSchema(inputSchema))
	arrowWriter.Write(inputRecord)
	arrowWriter.Close()
}
