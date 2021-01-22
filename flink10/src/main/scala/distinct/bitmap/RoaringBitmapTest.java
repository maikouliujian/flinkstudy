package distinct.bitmap;

import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

public class RoaringBitmapTest {

    public static void main(String[] args) {
        RoaringBitmap roaringBitmap = new RoaringBitmap();
        for (int i = 1; i <= 4096; i++) {
            roaringBitmap.add(i);
        }

        Roaring64NavigableMap roaring64NavigableMap=new Roaring64NavigableMap();
        roaring64NavigableMap.addLong(1233453453345L);
        roaring64NavigableMap.runOptimize();
        roaring64NavigableMap.getLongCardinality();
    }
}
