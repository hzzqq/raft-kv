// transport_codec_bench_test.go —— gob 编解码热点路径基准（cycle #116）。
//
// 每条 RPC 都要 Marshal/Unmarshal 请求与响应。运行：
//   go test -run='^$' -bench='BenchmarkGobCodec' -benchmem ./src/transport
package transport

import (
	"testing"
)

type benchKVReq struct {
	Key   string
	Value []byte
	Op    int
}

type benchKVResp struct {
	Value  []byte
	Err    string
	Leader int
}

func BenchmarkGobCodecMarshal(b *testing.B) {
	codec := GobCodec{}
	req := benchKVReq{Key: "foo", Value: make([]byte, 256), Op: 1}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := codec.Marshal(&req); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGobCodecUnmarshal(b *testing.B) {
	codec := GobCodec{}
	req := benchKVReq{Key: "foo", Value: make([]byte, 256), Op: 1}
	data, _ := codec.Marshal(&req)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var out benchKVReq
		if err := codec.Unmarshal(data, &out); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGobCodecRoundTrip(b *testing.B) {
	codec := GobCodec{}
	req := benchKVReq{Key: "foo", Value: make([]byte, 256), Op: 1}
	resp := benchKVResp{Value: make([]byte, 128), Leader: 2}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		rd, _ := codec.Marshal(&req)
		var out benchKVReq
		_ = codec.Unmarshal(rd, &out)
		wd, _ := codec.Marshal(&resp)
		var outr benchKVResp
		_ = codec.Unmarshal(wd, &outr)
	}
}
