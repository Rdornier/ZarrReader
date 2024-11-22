package loci.formats.services;

import java.io.File;

/*-
 * #%L
 * Implementation of Bio-Formats readers for the next-generation file formats
 * %%
 * Copyright (C) 2020 - 2022 Open Microscopy Environment
 * %%
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDERS OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 * #L%
 */

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.store.FilesystemStore;
import dev.zarr.zarrjava.v3.Group;
import dev.zarr.zarrjava.v3.DataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import dev.zarr.zarrjava.v3.Array;

import loci.common.services.AbstractService;
import loci.formats.FormatException;
import loci.formats.FormatTools;
import loci.formats.S3FileSystemStore;
import loci.formats.meta.MetadataRetrieve;

//TODO implements the Zarrv3service
public class ZarrV3JavaServiceImpl extends AbstractService
        implements ZarrV3Service  {
    // -- Constants --
    private static final Logger LOGGER = LoggerFactory.getLogger(ZarrV3JavaServiceImpl.class);
    public static final String NO_ZARR_MSG = "zarr-java is required to read Zarr V3 files.";

    // -- Fields --
    FilesystemStore fs;
    Array zarrArray;
    String currentId;
    /*Compressor zlibComp = CompressorFactory.create("zlib", "level", 8);  // 8 = compression level .. valid values 0 .. 9
    Compressor bloscComp = CompressorFactory.create("blosc", "cname", "lz4hc", "clevel", 7);
    Compressor nullComp = CompressorFactory.create("null");*/

    /**
     * Default constructor.
     */
    public ZarrV3JavaServiceImpl(String root) {
        checkClassDependency(Array.class);
        if (root != null && (root.toLowerCase().contains("s3:") || root.toLowerCase().contains("s3."))) {
            String[] pathSplit = root.toString().split(File.separator);
            if (S3FileSystemStore.ENDPOINT_PROTOCOL.contains(pathSplit[0].toLowerCase())) {
                //s3fs = new FilesystemStore(Paths.get(root)); //TODO implement the s3
            }
            else {
                LOGGER.warn("Zarr Reader is not using S3FileSystemStore as this is currently for use with S3 configured with a https endpoint");
            }
        }else{
            fs = new FilesystemStore(Paths.get(root));
        }
    }

    @Override
    public void open(String file) throws IOException, FormatException, ZarrException {
        currentId = file;
        zarrArray = getArray(file);
    }

    public void open(String id, Array array) {
        currentId = id;
        zarrArray = array;
    }

    //TODO remove it ; only for testing
    public Array getZarrArray(){ return zarrArray; }

    public Map<String, Object> getGroupAttr(String path) throws IOException, FormatException, ZarrException {
        return getGroup(path).metadata.attributes;
    }

    public Map<String, Object> getArrayAttr(String path) throws IOException, FormatException, ZarrException {
        return getArray(path).metadata.attributes;
    }

    public Set<String> getGroupKeys(String path) throws IOException, FormatException, ZarrException {
        return getGroup(path).storeHandle.list().collect(Collectors.toSet());
    }

    //TODO see if it is still needed => replacing by a Node ? Not sure it is possible
   public Set<String> getArrayKeys(String path) throws IOException, FormatException, ZarrException {
        return getArray(path).storeHandle.list().collect(Collectors.toSet());
    }

    public DataType getZarrPixelType(int pixType) {
        DataType pixelType = null;
        switch(pixType) {
            case FormatTools.INT8:
                pixelType = DataType.INT8;
                break;
            case FormatTools.INT16:
                pixelType = DataType.INT16;
                break;
            case FormatTools.INT32:
                pixelType = DataType.INT32;
                break;
            case FormatTools.UINT8:
                pixelType = DataType.UINT8;
                break;
            case FormatTools.UINT16:
                pixelType = DataType.UINT16;
                break;
            case FormatTools.UINT32:
                pixelType = DataType.UINT32;
                break;
            case FormatTools.FLOAT:
                pixelType = DataType.FLOAT32;
                break;
            case FormatTools.DOUBLE:
                pixelType = DataType.FLOAT64;
                break;
        }
        return(pixelType);
    }

    public int getOMEPixelType(DataType pixType) {

        int pixelType = -1;
        switch(pixType) {
       /*     case BOOL:
                pixelType = FormatTools.???;
                break;*/
            case INT8:
                pixelType = FormatTools.INT8;
                break;
            case INT16:
                pixelType = FormatTools.INT16;
                break;
            case INT32:
                pixelType = FormatTools.INT32;
                break;
            case INT64:
                pixelType = FormatTools.DOUBLE;
                break;
            case UINT8:
                pixelType = FormatTools.UINT8;
                break;
            case UINT16:
                pixelType = FormatTools.UINT16;
                break;
            case UINT32:
                pixelType = FormatTools.UINT32;
                break;
            case UINT64:
                pixelType = FormatTools.DOUBLE;
                break;
            case FLOAT32:
                pixelType = FormatTools.FLOAT;
                break;
            case FLOAT64:
                pixelType = FormatTools.DOUBLE;
                break;
            default:
                break;
        }
        return(pixelType);
    }

    @Override
    public String getNoZarrMsg() {
        return NO_ZARR_MSG;
    }

    @Override
    public long[] getShape() {
        if (zarrArray != null) return zarrArray.metadata.shape;
        return null;
    }

    @Override
    public int[] getChunkSize() {
        if (zarrArray != null) return zarrArray.metadata.chunkShape();
        return null;
    }

    @Override
    public int getPixelType() {
        if (zarrArray != null) return getOMEPixelType(zarrArray.metadata.dataType);
        return 0;
    }

    @Override
    //TODO see how to handle endiannesse
    public boolean isLittleEndian() {
      //  if (zarrArray != null) return (ByteOrder.LITTLE_ENDIAN);
        return false;
    }

    @Override
    public void close() throws IOException {
        zarrArray = null;
        currentId = null;
        fs = null;
        // TODO modify zarr-java such that it implements also a reader for local file systems
        //for s3 => need to close it
        /*if (s3fs != null) {
            s3fs.close();
        }*/
    }

    @Override
    public boolean isOpen() {
        return (zarrArray != null && currentId != null);
    }

    @Override
    public String getID() {
        return currentId;
    }

    @Override
    public Object readBytes(long[] shape, int[] offset) throws FormatException, IOException, ZarrException {
        if (zarrArray != null) {
            return zarrArray.read(shape, offset);
        }
        else throw new IOException("No Zarr file opened");
    }

    @Override
    public void saveBytes(Object data, long[] offset) throws FormatException, IOException {
        if (zarrArray != null) {
            if (data instanceof ucar.ma2.Array) {
                zarrArray.write(offset, (ucar.ma2.Array) data);
                return;
            }
            throw new IOException("No Zarr file opened");
        }
    }

    @Override
    public void create(String file, MetadataRetrieve meta, int[] chunks, Compression compression) throws IOException {
       /* int seriesCount = meta.getImageCount();
        int resolutionCount = 1;



        for (int res = 0; res < seriesCount; res++) {
            String resolutionPath = file + "/" + res;
            LOGGER.info("opening v2 array: {}", resolutionPath);

            int[] originalChunkSizes = tile.getChunks();
            if (requestedChunkSize != null) {
                originalChunkSizes = requestedChunkSize;
            }

            int x = meta.getPixelsSizeX(res).getValue().intValue();
            int y = meta.getPixelsSizeY(res).getValue().intValue();
            int z = meta.getPixelsSizeZ(res).getValue().intValue();
            int c = meta.getPixelsSizeC(res).getValue().intValue();
            int t = meta.getPixelsSizeT(res).getValue().intValue();

            int[] shape = new int[]{x, y, z, c, t};

            int[] chunkSizes = new int[originalChunkSizes.length];
            System.arraycopy(originalChunkSizes, 0, chunkSizes, 0, chunkSizes.length);

            int[] gridPosition = new int[]{0, 0, 0, 0, 0};

            com.bc.zarr.DataType type = tile.getDataType();

            // create the v3 array for writing

            CodecBuilder codecBuilder = new CodecBuilder(getV3Type(type));
            if (shardConfig != null) {
                switch (shardConfig) {
                    case SINGLE:
                        // single shard covering the whole image
                        // internal chunk sizes remain the same as in input data
                        chunkSizes = shape;
                        break;
                    case CHUNK:
                        // exactly one shard per chunk
                        // no changes needed
                        break;
                    case SUPERCHUNK:
                        // each shard covers 2x2 chunks in XY
                        chunkSizes[4] *= 2;
                        chunkSizes[3] *= 2;

                        // shard across other dimensions too, but only
                        // if the dimension is greater than the chunk size
                        for (int i = 0; i <= 2; i++) {
                            if (shape[i] > chunkSizes[i]) {
                                chunkSizes[i] *= 2;
                            }
                        }
                        break;
                    case CUSTOM:
                        chunkSizes = requestedShard;
                        break;
                }

                if (chunkAndShardCompatible(originalChunkSizes, chunkSizes, shape)) {
                    codecBuilder = codecBuilder.withSharding(originalChunkSizes);
                } else {
                    LOGGER.warn("Skipping sharding due to incompatible sizes");
                    LOGGER.debug("  tried chunk={}, shard={}",
                            Arrays.toString(originalChunkSizes), Arrays.toString(chunkSizes));
                    chunkSizes = originalChunkSizes;
                }
            }
            if (codecs != null) {
                for (String codecName : codecs) {
                    if (codecName.equals("crc32")) {
                        codecBuilder = codecBuilder.withCrc32c();
                    } else if (codecName.equals("zstd")) {
                        codecBuilder = codecBuilder.withZstd();
                    } else if (codecName.equals("gzip")) {
                        codecBuilder = codecBuilder.withGzip();
                    } else if (codecName.equals("blosc")) {
                        codecBuilder = codecBuilder.withBlosc();
                    }
                }
            }
            final CodecBuilder builder = codecBuilder;

            StoreHandle v3ArrayHandle = outputStore.resolve(seriesGroupKey, columnKey, fieldKey, String.valueOf(res));
            LOGGER.debug("opening v3 array: {}", v3ArrayHandle);
            Array outputArray = Array.create(v3ArrayHandle,
                    Array.metadataBuilder()
                            .withShape(Utils.toLongArray(shape))
                            .withDataType(getV3Type(type))
                            .withChunkShape(chunkSizes) // if sharding is used, this will be the shard size
                            .withFillValue(255)
                            .withCodecs(c -> builder)
                            .build()
            );


        }*/



        /*ArrayParams params = new ArrayParams();
        params.chunks(chunks);
        params.compressor(nullComp);
*/
        /*boolean isLittleEndian = !meta.getPixelsBigEndian(0);
        if (isLittleEndian) {
            params.byteOrder(ByteOrder.LITTLE_ENDIAN);
        }*/

       /* //int x = meta.getPixelsSizeX(0).getValue().intValue();
        //int y = meta.getPixelsSizeY(0).getValue().intValue();
        //int z = meta.getPixelsSizeZ(0).getValue().intValue();
        //int c = meta.getPixelsSizeC(0).getValue().intValue();
        //int t = meta.getPixelsSizeT(0).getValue().intValue();
        // c /= meta.getChannelSamplesPerPixel(0, 0).getValue().intValue();
        //int [] shape = {x, y, z, c, t};
        //params.shape(shape);

        int pixelType = FormatTools.pixelTypeFromString(meta.getPixelsType(0).toString());
        DataType zarrPixelType = getZarrPixelType(pixelType);
      //  int bytes = FormatTools.getBytesPerPixel(pixelType);
       // params.dataType(zarrPixelType);

        if (seriesCount > 1) {
            ZarrGroup root = ZarrGroup.create(file);
            ZarrGroup currentGroup = root;
            for (int i = 0; i < seriesCount; i++) {
                x = meta.getPixelsSizeX(i).getValue().intValue();
                y = meta.getPixelsSizeY(i).getValue().intValue();
                z = meta.getPixelsSizeZ(i).getValue().intValue();
                c = meta.getPixelsSizeC(i).getValue().intValue();
                t = meta.getPixelsSizeT(i).getValue().intValue();
                //  c /= meta.getChannelSamplesPerPixel(i, 0).getValue().intValue();
                shape = new int[]{x, y, z, c, t};
                params.shape(shape);

                pixelType = FormatTools.pixelTypeFromString(meta.getPixelsType(i).toString());
                zarrPixelType = getZarrPixelType(pixelType);
                params.dataType(zarrPixelType);

                isLittleEndian = !meta.getPixelsBigEndian(i);
                if (isLittleEndian) {
                    params.byteOrder(ByteOrder.LITTLE_ENDIAN);
                }

                if (meta instanceof IPyramidStore) {
                    resolutionCount = ((IPyramidStore) meta).getResolutionCount(i);
                }
                if (resolutionCount > 1) {
                    currentGroup = root.createSubGroup("Series"+i);
                    for (int j = 0; j < resolutionCount; j++) {
                        zarrArray = currentGroup.createArray("Resolution"+j, params);
                    }
                }
                else {
                    zarrArray = currentGroup.createArray("Series"+i, params);
                }
            }
        }
        else {
            zarrArray = ZarrArray.create(file, params);
        }
        currentId = file;*/
    }

    @Override
    public void create(String id, MetadataRetrieve meta, int[] chunks) throws IOException {
        create(id, meta, chunks, Compression.NONE);
    }

    private String stripZarrRoot(String path) {
        return path.substring(path.indexOf(".zarr")+5);
    }

    private String getZarrRoot(String path) {
        return path.substring(0, path.indexOf(".zarr")+5);
    }

    private Group getGroup(String path) throws IOException, ZarrException {
        if (fs == null) {
            throw new ZarrException("Cannot read the Zarr group at path '"+path+"'");
        }
        else {
            //TODO see if it is s3 specifications or just remove it ?
            //s3fs.updateRoot(getZarrRoot(s3fs.getRoot()) + stripZarrRoot(path));
            return Group.open(fs.resolve(path));
        }
    }

    private Array getArray(String path) throws IOException, ZarrException {
        if (fs == null) {
           throw new ZarrException("Cannot read the Zarr array at path '"+path+"'");
        }
        else {
            //TODO see if it is s3 specifications or just remove it ?
            //s3fs.updateRoot(getZarrRoot(s3fs.getRoot()) + stripZarrRoot(path));
            return Array.open(fs.resolve(path));
        }
    }

    //TODO add this for other stores
    public boolean usingS3FileSystemStore() {
        return fs != null;
    }
}
