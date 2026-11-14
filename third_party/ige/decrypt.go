package ige

import (
	"crypto/cipher"
	"encoding/binary"

	"github.com/go-faster/xor"
)

// NewIGEDecrypter returns an IGE cipher.BlockMode which decrypts using IGE and
// the given cipher.Block.
//
// Note: iv must contain two iv values for IGE (concatenated), otherwise this
// function will panic. See ErrInvalidIV for more information.
func NewIGEDecrypter(b cipher.Block, iv []byte) IGE {
	if err := checkIV(b, iv); err != nil {
		panic(err.Error())
	}

	return (*igeDecrypter)(newIGE(b, iv))
}

type igeDecrypter ige

func (i *igeDecrypter) BlockSize() int {
	return i.block.BlockSize()
}

func (i *igeDecrypter) CryptBlocks(dst, src []byte) {
	DecryptBlocks(i.block, i.iv, dst, src)
}

// DecryptBlocks is a simple shorthand for IGE decrypting.
// Note: unlike NewIGEDecrypter, DecryptBlocks does NOT COPY iv.
// So you must not modify passed iv.
func DecryptBlocks(block cipher.Block, iv, dst, src []byte) {
	if err := checkIV(block, iv); err != nil {
		panic(err.Error())
	}
	if len(src)%block.BlockSize() != 0 {
		panic("src not full blocks")
	}
	if len(dst) < len(src) {
		panic("len(dst) < len(src)")
	}

	if useFast16 && block.BlockSize() == 16 {
		decryptBlocks16(block, iv, dst, src)
		return
	}
	decryptBlocksGeneric(block, iv, dst, src)
}

// decryptBlocks16 is DecryptBlocks for 16-byte ciphers (AES), keeping the IGE
// chaining state in registers instead of calling xor.Bytes twice per block.
// mtflux change: the per-block xor.Bytes calls were ~20% of download CPU on
// arm64.
func decryptBlocks16(block cipher.Block, iv, dst, src []byte) {
	le := binary.LittleEndian
	c0, c1 := le.Uint64(iv[0:8]), le.Uint64(iv[8:16])    // previous ciphertext
	m0, m1 := le.Uint64(iv[16:24]), le.Uint64(iv[24:32]) // previous plaintext

	for o := 0; o+16 <= len(src); o += 16 {
		s := src[o : o+16 : o+16]
		d := dst[o : o+16 : o+16]
		s0, s1 := le.Uint64(s[0:8]), le.Uint64(s[8:16])

		le.PutUint64(d[0:8], s0^m0)
		le.PutUint64(d[8:16], s1^m1)
		block.Decrypt(d, d)
		p0, p1 := le.Uint64(d[0:8])^c0, le.Uint64(d[8:16])^c1
		le.PutUint64(d[0:8], p0)
		le.PutUint64(d[8:16], p1)

		m0, m1 = p0, p1
		c0, c1 = s0, s1
	}
}

func decryptBlocksGeneric(block cipher.Block, iv, dst, src []byte) {
	b := block.BlockSize()
	c := iv[:b]
	m := iv[b:]

	for o := 0; o < len(src); o += b {
		t := src[o : o+b : o+b]

		xor.Bytes(dst[o:o+b:o+b], src[o:o+b:o+b], m)
		block.Decrypt(dst[o:o+b:o+b], dst[o:o+b:o+b])
		xor.Bytes(dst[o:o+b:o+b], dst[o:o+b:o+b], c)

		m = dst[o : o+b]
		c = t
	}
}
