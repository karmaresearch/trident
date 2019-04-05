package karmaresearch.trident;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;


public class Trident
{
    public long myTrident;
    public long myTridentLayer;

    static {
        loadLibrary("kognac-core");
        loadLibrary("trident-core");
        loadLibrary("trident-sparql");
        loadLibrary("trident-java");
        loadLibrary("trident-web");
    };

    public Trident() {
        myTrident = 0;
        myTridentLayer = 0;
    }

    private static void loadLibrary(String s) {
        // First try to just load the shared library.
        try {
            System.loadLibrary(s);
        } catch (Throwable ex) {
            // Did not work, now try to load it from the same directory as the
            // jar file. First determine prefix and suffix, depending on OS.

            // First determine jar file;
            File jarFile;
            try {
                jarFile = new File(Trident.class.getProtectionDomain()
                        .getCodeSource().getLocation().toURI());
            } catch (Throwable e) {
                throw (UnsatisfiedLinkError) new UnsatisfiedLinkError(
                        "while loading " + s + ": " + e.getMessage())
                    .initCause(e.getCause());
            }

            // Next, determine OS.
            String os = System.getProperty("os.name");
            String nativeSuffix = ".so";
            String nativePrefix = "lib";
            if (os != null) {
                os = os.toLowerCase();
                if (os.contains("windows")) {
                    nativePrefix = "";
                    nativeSuffix = ".dll";
                } else if (os.contains("mac")) {
                    nativeSuffix = ".dylib";
                }
            }

            // Determine library name.
            String libName = nativePrefix + s + nativeSuffix;

            try {
                loadFromDir(jarFile, libName);
            } catch (Throwable e) {
                try {
                    loadFromJar(jarFile, libName, os);
                } catch (Throwable e1) {
                    // System.err.println("Could not load library " + s
                    // + ", exceptions are temporarily disabled to accomodate
                    // windows version");
                    throw (UnsatisfiedLinkError) new UnsatisfiedLinkError(
                            "while loading " + s + ": " + e1.getMessage())
                        .initCause(e1.getCause());
                }
            }
        }
    }

    private static void loadFromDir(File jarFile, String libName) {
        String dir = jarFile.getParent();
        if (dir == null) {
            dir = ".";
        }
        // Only support one size, i.e. 64-bit?
        String lib = dir + File.separator + libName;
        System.load(lib);
    }

    private static void loadFromJar(File jarFile, String libName, String os)
            throws IOException {
            InputStream is = new BufferedInputStream(
                    Trident.class.getResourceAsStream(libName));
            File targetDir = Files.createTempDirectory("Trident-tmp").toFile();
            targetDir.deleteOnExit();
            File target = new File(targetDir, libName);
            target.deleteOnExit();
            targetDir.deleteOnExit();
            Files.copy(is, target.toPath(), StandardCopyOption.REPLACE_EXISTING);
            try {
                System.load(target.getAbsolutePath());
            } finally {
                if (os == null || !os.contains("windows")) {
                    // If not on windows, we can delete the files now.
                    target.delete();
                    targetDir.delete();
                }
            }
    }

    public native void load(String db);

    public native String sparql(String query);

    public native void unload();
}
