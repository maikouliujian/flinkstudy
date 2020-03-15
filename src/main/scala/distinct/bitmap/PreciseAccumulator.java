package distinct.bitmap;

import org.roaringbitmap.longlong.Roaring64NavigableMap;

public class PreciseAccumulator{

    private Roaring64NavigableMap bitmap;

    public PreciseAccumulator(){
        bitmap=new Roaring64NavigableMap();
    }

    public void add(long id){
        bitmap.addLong(id);
    }

    public long getCardinality(){
        return bitmap.getLongCardinality();
    }
}
