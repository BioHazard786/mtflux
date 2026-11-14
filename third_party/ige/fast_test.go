package ige

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"testing"
)

// The 16-byte fast paths must match the original generic loops byte for byte.
func TestFastPathMatchesGeneric(t *testing.T) {
	key := make([]byte, 32)
	iv := make([]byte, 32)
	for _, n := range []int{16, 32, 48, 4096, 1 << 20} {
		_, _ = rand.Read(key)
		_, _ = rand.Read(iv)
		block, err := aes.NewCipher(key)
		if err != nil {
			t.Fatal(err)
		}
		src := make([]byte, n)
		_, _ = rand.Read(src)

		want := make([]byte, n)
		got := make([]byte, n)
		decryptBlocksGeneric(block, iv, want, src)
		decryptBlocks16(block, iv, got, src)
		if !bytes.Equal(want, got) {
			t.Fatalf("decrypt mismatch at n=%d", n)
		}

		encryptBlocksGeneric(block, iv, want, src)
		encryptBlocks16(block, iv, got, src)
		if !bytes.Equal(want, got) {
			t.Fatalf("encrypt mismatch at n=%d", n)
		}

		// Round trip through the public API.
		ct := make([]byte, n)
		pt := make([]byte, n)
		EncryptBlocks(block, iv, ct, src)
		DecryptAES256Blocks(key, iv, pt, ct)
		if !bytes.Equal(pt, src) {
			t.Fatalf("round trip mismatch at n=%d", n)
		}
	}
}

func bench1MiB(b *testing.B, f func(block cipher.Block, iv, dst, src []byte)) {
	key := make([]byte, 32)
	iv := make([]byte, 32)
	block, _ := aes.NewCipher(key)
	src := make([]byte, 1<<20)
	dst := make([]byte, 1<<20)
	b.SetBytes(int64(len(src)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f(block, iv, dst, src)
	}
}

func BenchmarkDecrypt1MiBGeneric(b *testing.B) { bench1MiB(b, decryptBlocksGeneric) }
func BenchmarkDecrypt1MiBFast(b *testing.B)    { bench1MiB(b, decryptBlocks16) }

// Lower bound: AES-256 block decryption alone, chained so each block depends
// on the previous one like IGE does (no XORs, no IGE bookkeeping).
func aesChain(block cipher.Block, iv, dst, src []byte) {
	prev := iv[:16]
	for o := 0; o+16 <= len(src); o += 16 {
		d := dst[o : o+16 : o+16]
		block.Decrypt(d, prev)
		prev = d
	}
}

func BenchmarkDecrypt1MiBAESOnlyChained(b *testing.B) { bench1MiB(b, aesChain) }
