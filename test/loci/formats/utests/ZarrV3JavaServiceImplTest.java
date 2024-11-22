package loci.formats.utests;

import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.store.FilesystemStore;
import dev.zarr.zarrjava.v3.Array;
import dev.zarr.zarrjava.v3.DataType;
import dev.zarr.zarrjava.v3.Group;
import loci.formats.FormatException;
import loci.formats.services.ZarrV3JavaServiceImpl;
import org.junit.jupiter.api.Assertions;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ZarrV3JavaServiceImplTest {

    private String outputFolder = "C:\\Users\\dornier\\Downloads";
    private String zarrV3file = outputFolder  + File.separator + "Fluosharded.ome.zarr";
    private String arrayV3 = zarrV3file + File.separator + "0\\2";

    @Test
    public void readZarrJSONOnArrayLevelTest() throws ZarrException, IOException, FormatException {
        ZarrV3JavaServiceImpl imp = new ZarrV3JavaServiceImpl(zarrV3file);
        imp.open(arrayV3);
        Array zarrarray =  imp.getZarrArray();
        System.out.println(zarrarray.metadata.shape[0]);
        System.out.println(imp.getArrayAttr(arrayV3));
        System.out.println(imp.getGroupAttr(zarrV3file));
        System.out.println(imp.getArrayKeys(arrayV3));
        System.out.println(imp.getZarrPixelType(imp.getPixelType()));
        int[] offset = new int[]{0, 0, 0, 0, 0};
        System.out.println(imp.readBytes(imp.getShape(), offset ).toString());
    }

    @Test
    public void testV3Group() throws IOException, ZarrException {
        FilesystemStore fsStore = new FilesystemStore(Paths.get(outputFolder + File.separator + "testoutput"));

        Map<String, Object> attributes = new HashMap<>();
        attributes.put("hello", "world");

        Group group = Group.create(fsStore.resolve("testgroup"));
        Group group2 = group.createGroup("test2", attributes);
        Array array = group2.createArray("array", b ->
                b.withShape(10, 10)
                        .withDataType(DataType.UINT8)
                        .withChunkShape(5, 5)
        );
        array.write(new long[]{2, 2}, ucar.ma2.Array.factory(ucar.ma2.DataType.UBYTE, new int[]{8, 8}));

        Assertions.assertArrayEquals(((Array) ((Group) group.listAsArray()[0]).listAsArray()[0]).metadata.chunkShape(), new int[]{5, 5});
    }

}
