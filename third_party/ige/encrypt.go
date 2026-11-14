package ige

import (
	"crypto/cipher"
	"encoding/binary"

	"github.com/go-faster/xor"
)

// NewIGEEncrypter returns an IGE cipher.BlockMode which encrypts using IGE and
// the given cipher.Block.
//
// Note: iv must contain two iv values for IGE (concatenated), otherwise this
// function will panic. See ErrInvalidIV for more information.
func NewIGEEncrypter(b cipher.Block, iv []byte) IGE {
	if err := checkIV(b, iv); err != nil {
		panic(err.Error())
	}

	return (*igeEncrypter)(newIGE(b, iv))
}

type igeEncrypter ige

func (i *igeEncrypter) BlockSize() int {
	return i.block.BlockSize()
}

func (i *igeEncrypter) CryptBlocks(dst, src []byte) {
	EncryptBlocks(i.block, i.iv, dst, src)
}

// EncryptBlocks is a simple shorthand for IGE encrypting.
// Note: unlike NewIGEEncrypter, EncryptBlocks does NOT COPY iv.
// So you must not modify passed iv.
func EncryptBlocks(block cipher.Block, iv, dst, src []byte) {
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
		encryptBlocks16(block, iv, dst, src)
		return
	}
	encryptBlocksGeneric(block, iv, dst, src)
}

// encryptBlocks16 is EncryptBlocks for 16-byte ciphers (AES); see
// decryptBlocks16.
func encryptBlocks16(block cipher.Block, iv, dst, src []byte) {
	le := binary.LittleEndian
	c0, c1 := le.Uint64(iv[0:8]), le.Uint64(iv[8:16])    // previous ciphertext
	m0, m1 := le.Uint64(iv[16:24]), le.Uint64(iv[24:32]) // previous plaintext

	for o := 0; o+16 <= len(src); o += 16 {
		s := src[o : o+16 : o+16]
		d := dst[o : o+16 : o+16]
		s0, s1 := le.Uint64(s[0:8]), le.Uint64(s[8:16])

		le.PutUint64(d[0:8], s0^c0)
		le.PutUint64(d[8:16], s1^c1)
		block.Encrypt(d, d)
		x0, x1 := le.Uint64(d[0:8])^m0, le.Uint64(d[8:16])^m1
		le.PutUint64(d[0:8], x0)
		le.PutUint64(d[8:16], x1)

		c0, c1 = x0, x1
		m0, m1 = s0, s1
	}
}

func encryptBlocksGeneric(block cipher.Block, iv, dst, src []byte) {
	b := block.BlockSize()
	c := iv[:b]
	m := iv[b:]

	for o := 0; o < len(src); o += b {
		xor.Bytes(dst[o:o+b:o+b], src[o:o+b:o+b], c)
		block.Encrypt(dst[o:o+b:o+b], dst[o:o+b:o+b])
		xor.Bytes(dst[o:o+b:o+b], dst[o:o+b:o+b], m)

		c = dst[o : o+b : o+b]
		m = src[o : o+b : o+b]
	}
}
