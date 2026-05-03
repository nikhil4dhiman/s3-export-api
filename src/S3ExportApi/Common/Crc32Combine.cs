using System.IO.Hashing;

namespace S3ExportApi.Common;

/// <summary>
/// Provides CRC32 combine functionality: given crc32(A) and crc32(B) plus len(B),
/// compute crc32(A || B) without re-reading the data.
/// Based on the zlib crc32_combine algorithm (matrix squaring in GF(2)).
/// </summary>
public static class Crc32Combine
{
    private const uint Poly = 0xEDB88320u;

    /// <summary>Combine two CRC32 values where <paramref name="crc2"/> covers <paramref name="len2"/> bytes.</summary>
    public static uint Combine(uint crc1, uint crc2, long len2)
    {
        if (len2 == 0) return crc1;

        // Build the operator for "zero bytes" applied len2 times
        Span<uint> even = stackalloc uint[32];
        Span<uint> odd = stackalloc uint[32];

        // odd = operator for one zero bit
        odd[0] = Poly;
        uint row = 1;
        for (int i = 1; i < 32; i++)
        {
            odd[i] = row;
            row <<= 1;
        }

        // Apply len2 zeros to crc1
        long remaining = len2;
        do
        {
            // Square odd into even
            MatrixSquare(even, odd);
            if ((remaining & 1) != 0)
                crc1 = MatrixMultiply(even, crc1);
            remaining >>= 1;
            if (remaining == 0) break;

            // Square even into odd
            MatrixSquare(odd, even);
            if ((remaining & 1) != 0)
                crc1 = MatrixMultiply(odd, crc1);
            remaining >>= 1;
        } while (remaining != 0);

        return crc1 ^ crc2;
    }

    private static void MatrixSquare(Span<uint> result, ReadOnlySpan<uint> mat)
    {
        for (int i = 0; i < 32; i++)
            result[i] = MatrixMultiply(mat, mat[i]);
    }

    private static uint MatrixMultiply(ReadOnlySpan<uint> mat, uint vec)
    {
        uint sum = 0;
        int i = 0;
        while (vec != 0)
        {
            if ((vec & 1) != 0)
                sum ^= mat[i];
            vec >>= 1;
            i++;
        }
        return sum;
    }

    /// <summary>Compute CRC32 of a byte span (convenience wrapper around System.IO.Hashing).</summary>
    public static uint Compute(ReadOnlySpan<byte> data)
    {
        return Crc32.HashToUInt32(data);
    }
}
