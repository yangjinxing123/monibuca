package mpegps

import (
	"io"

	mpegts "m7s.live/v5/pkg/format/ts"
	"m7s.live/v5/pkg/util"
)

type MpegpsPESFrame struct {
	StreamType byte // Stream type (e.g., video, audio)
	mpegts.MpegPESHeader
}

func (frame *MpegpsPESFrame) WritePESPacket(payload util.Memory, allocator *util.RecyclableMemory) (err error) {
	var pesHeadItem util.Buffer
	pesHeadItem, err = frame.WritePESHeader(payload.Size)
	if err != nil {
		return
	}
	pesBuffers := util.NewMemory(pesHeadItem)
	payload.Range(pesBuffers.PushOne)
	pesReader := pesBuffers.NewReader()
	for pesReader.Length > 0 {
		currentPESPayload := min(pesReader.Length, MaxPESPayloadSize)
		// 申请输出缓冲
		var outputMemory util.Buffer = allocator.NextN(PSPackHeaderSize + currentPESPayload)
		outputMemory.Reset()
		MuxPSHeader(&outputMemory)

		io.CopyN(&outputMemory, &pesReader, int64(currentPESPayload))
	}

	return nil
}
