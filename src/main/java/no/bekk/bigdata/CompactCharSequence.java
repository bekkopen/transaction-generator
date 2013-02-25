package no.bekk.bigdata;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;

/**
 * By using this instead of String, we save a significant amount of memory (and can fit at least 35% more in the same space).
 */
public class CompactCharSequence implements CharSequence, Serializable
{
    static final long serialVersionUID = 1L;

    private static final String ENCODING = "ISO-8859-1";
    private final int offset;
    private final int end;
    private final byte[] data;

    private CompactCharSequence(byte[] data, int offset, int end)
    {
        this.data = data;
        this.offset = offset;
        this.end = end;
    }

    public CompactCharSequence(String str)
    {
        try
        {
            data = str.getBytes(ENCODING);
            offset = 0;
            end = data.length;
        } catch (UnsupportedEncodingException e)
        {
            throw new RuntimeException("Unexpected: " + ENCODING + " not supported!");
        }
    }

    public char charAt(int index)
    {
        int ix = index + offset;
        if (ix >= end)
        {
            throw new StringIndexOutOfBoundsException("Invalid index " +
                    index + " length " + length());
        }
        return (char) (data[ix] & 0xff);
    }

    public int length()
    {
        return end - offset;
    }

    public CharSequence subSequence(int start, int end)
    {
        if (start < 0 || end >= (this.end - offset))
        {
            throw new IllegalArgumentException("Illegal range " +
                    start + "-" + end + " for sequence of length " + length());
        }
        return new CompactCharSequence(data, start + offset, end + offset);
    }

    public String toString()
    {
        try
        {
            return new String(data, offset, end - offset, ENCODING);
        } catch (UnsupportedEncodingException e)
        {
            throw new RuntimeException("Unexpected: " + ENCODING + " not supported");
        }
    }

    public boolean equals(CompactCharSequence other){
        return this != null && this.toString().equals(other.toString());
    }
}