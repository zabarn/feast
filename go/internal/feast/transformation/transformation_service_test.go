package transformation

import (
  "testing"
  "fmt"

	"github.com/stretchr/testify/assert"

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
		inputFields = append(inputFields, arrow.Field{Name: fmt.Sprintf("Field %d", i), Type: &arrow.Int64Type{}})
		inputColumns = append(inputColumns, dummyColumn.NewArray())
    switch col := inputColumns[i].(type) {
    case *array.Int64:
      fmt.Println(col.Int64Values())
    case *array.Int32:
      fmt.Println(col.Int32Values())
    case *array.String:
      fmt.Println(col.String())
    case *array.Float32:
      fmt.Println(col.Float32Values())
    case *array.Float64:
      fmt.Println(col.Float64Values())
    }
	}

  inputSchema := arrow.NewSchema(inputFields, nil)
	inputRecord := array.NewRecord(inputSchema, inputColumns, int64(10))
	defer inputRecord.Release()

	recordValueWriter := new(ByteSliceWriter)
	arrowWriter, _ := ipc.NewFileWriter(recordValueWriter, ipc.WithSchema(inputSchema))
	arrowWriter.Write(inputRecord)
	arrowWriter.Close()
  for i := range inputRecord.Columns() {
    assert.Equal(t, fmt.Sprintf("Field %d", i), inputRecord.ColumnName(i))
	}
}
