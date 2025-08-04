package pkg

import (
	"bytes"
	"math/rand"
	"testing"

	"m7s.live/v5/pkg/util"
)

func bytesFromMemory(m util.Memory) []byte {
	if m.Size == 0 {
		return nil
	}
	out := make([]byte, 0, m.Size)
	for _, b := range m.Buffers {
		out = append(out, b...)
	}
	return out
}

func TestAnnexBReader_ReadNALU_Basic(t *testing.T) {
	allocator := util.NewScalableMemoryAllocator(1 << 12)
	defer allocator.Recycle()

	reader := NewAnnexBReader(allocator)

	// 3 个 NALU，分别使用 4 字节、3 字节、4 字节起始码
	expected1 := []byte{0x67, 0x42, 0x00, 0x1E}
	expected2 := []byte{0x68, 0xCE, 0x3C, 0x80}
	expected3 := []byte{0x65, 0x88, 0x84, 0x00}

	buf := append([]byte{0x00, 0x00, 0x00, 0x01}, expected1...)
	buf = append(buf, append([]byte{0x00, 0x00, 0x01}, expected2...)...)
	buf = append(buf, append([]byte{0x00, 0x00, 0x00, 0x01}, expected3...)...)

	reader.AppendBuffer(buf)

	// 读取并校验 3 个 NALU（不包含起始码）
	var n util.Memory
	if err := reader.ReadNALU(nil, &n); err != nil {
		t.Fatalf("read nalu 1: %v", err)
	}
	if !bytes.Equal(bytesFromMemory(n), expected1) {
		t.Fatalf("nalu1 mismatch")
	}

	n = util.Memory{}
	if err := reader.ReadNALU(nil, &n); err != nil {
		t.Fatalf("read nalu 2: %v", err)
	}
	if !bytes.Equal(bytesFromMemory(n), expected2) {
		t.Fatalf("nalu2 mismatch")
	}

	n = util.Memory{}
	if err := reader.ReadNALU(nil, &n); err != nil {
		t.Fatalf("read nalu 3: %v", err)
	}
	if !bytes.Equal(bytesFromMemory(n), expected3) {
		t.Fatalf("nalu3 mismatch")
	}

	// 再读一次应无更多起始码，返回 nil 错误且长度为 0
	if err := reader.ReadNALU(nil, &n); err != nil {
		t.Fatalf("expected nil error when no more nalu, got: %v", err)
	}
	if reader.Length != 0 {
		t.Fatalf("expected length 0 after reading all, got %d", reader.Length)
	}
}

func TestAnnexBReader_AppendBuffer_MultiChunk_Random(t *testing.T) {
	allocator := util.NewScalableMemoryAllocator(1 << 12)
	defer allocator.Recycle()

	reader := NewAnnexBReader(allocator)

	rng := rand.New(rand.NewSource(1)) // 固定种子，保证可复现

	// 生成随机 NALU（仅负载部分），并构造 AnnexB 数据（随机 3/4 字节起始码）
	numNALU := 12
	expectedPayloads := make([][]byte, 0, numNALU)
	fullStream := make([]byte, 0, 1024)

	for i := 0; i < numNALU; i++ {
		payloadLen := 1 + rng.Intn(32)
		payload := make([]byte, payloadLen)
		for j := 0; j < payloadLen; j++ {
			payload[j] = byte(rng.Intn(256))
		}
		expectedPayloads = append(expectedPayloads, payload)

		if rng.Intn(2) == 0 {
			fullStream = append(fullStream, 0x00, 0x00, 0x01)
		} else {
			fullStream = append(fullStream, 0x00, 0x00, 0x00, 0x01)
		}
		fullStream = append(fullStream, payload...)
	}

	// 随机切割为多段并 AppendBuffer
	for i := 0; i < len(fullStream); {
		// 每段长度 1..7 字节（或剩余长度）
		maxStep := 7
		remain := len(fullStream) - i
		step := 1 + rng.Intn(maxStep)
		if step > remain {
			step = remain
		}
		reader.AppendBuffer(fullStream[i : i+step])
		i += step
	}

	// 依次读取并校验
	for idx, expected := range expectedPayloads {
		var n util.Memory
		if err := reader.ReadNALU(nil, &n); err != nil {
			t.Fatalf("read nalu %d: %v", idx+1, err)
		}
		got := bytesFromMemory(n)
		if !bytes.Equal(got, expected) {
			t.Fatalf("nalu %d mismatch: expected %d bytes, got %d bytes", idx+1, len(expected), len(got))
		}
	}

	// 没有更多 NALU
	var n util.Memory
	if err := reader.ReadNALU(nil, &n); err != nil {
		t.Fatalf("expected nil error when no more nalu, got: %v", err)
	}
}
