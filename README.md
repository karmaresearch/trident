# Trident

## Installation on Ubuntu (or similar Linux Distros)

The installation should be fairly easy on Ubuntu (or on similar Linux distro
that provide packages of commonly-used libraries). Make sure you have
the following packages installed (with apt-get or similar):

git
libbost-all-dev
liblz4-dev
libtbb-dev
libsparsehash-dev
cmake

Then, create a directory in the main source directory (e.g build), move it
there and launch the command (see below for some special parameters like DEBUG
mode, etc.)

```
cmake ..
```

This will create some configuration files. Then, a simple

```
make
```

should compile everything. The compilation process creates a: 1) static library called
"libtrident.a", 2) a Python module called "trident.so" (remember to add this
path to the environmental variable PYTHONPATH if you want to use Trident with
Python), 3) an executable called "trident_exec". 


## Installation on MacOS

The process is very similar to Ubuntu. You can install the libraries that
are mentioned with Homebrew.

## Installation on Windows

I'm very sorry but I do not have a Windows machine, so I've never compiled
Trident with this OS. I hope that the information that I gave about the
compilation with other environments will be helpful to install Trident under
this system.

## More advanced info on compilation

The compilation relies on CMake to create the executable and libraries. CMake
requires that you create a new directory and run the program "cmake" inside it
pointing to another location that contains the file CMakeLists.txt. This
program will create all sorts of files which will instruct "make" to build
everything.  Suppose we create a directory "build" inside the main source tree
with:

```
mkdir build
```

Then we move there

```
cd build
```

and create all the setup files to compile the program

```
cmake ..
```

If you want to create a debug version of the program, add the following
parameter to cmake:

```
cmake .. -DCMAKE_BUILD_TYPE=Debug
```

Trident requires that the Boost libraries are compiled with multi-threading
support and should be available in accessable locations. Trident also requires
Intel's Thread Building Block libraries. Trident uses other libraries (e.g.
Google's sparsehash library, LZ4, etc.), but if these other libraries are not
available then CMake will (or should) automatically download and compile them.

Trident heavily relies on KOGNAC (an advanced compression library) to encode
the knowledge graph and for various other routines. KOGNAC is available at <a
href="https://github.com/jrbn/kognac">here</a>. The default behaviour of
Trident is to download and compile KOGNAC during the building process. If, for
any reason, you want to use an pre-existing version of KOGNAC, you can point
Trident to the locations of KOGNAC with the two parameters to pass to cmake:

```
-DKOGNAC_LIB=<absolute path to the KOGNAC library>/libkognac.a
-DKOGNAC_INC=<absolute path to the KOGNAC include files>
```

Remember a few things: If your system uses a version of Boost which is older
than 1.62, then you might get many warnings during the compilation. These
warnings are not problematic since they relate to a bug in the Boost libraries.

## Troubleshooting

### It does not compile!

I'm sorry to hear this. I did my best to make the compilation process as robust
as possible, but things can always go wrong.

One reason could be that you are using a old compiler. Trident relies on C++11
so it requires a modern compiler. Also, it assumes that the system can handle
64bit numbers well. This means that a "long" should correspond to 64bit while a
"int" points to a 32bit number.

Another reason for failure could be that in the source code of Trident there is
also a (minimally modified) version of the SNAP library. This library must be
compiled first, otherwise Trident won't be able to compile. The CMake process
should automatically take care of this, but if things go wrong here you have a
problem. To check SNAP has been correctly compiled, make sure that the file
"snap/snap-core/libsnap.a" exists. 

In order to compile, Trident also requires Python3, so the headers should be
installed. Typically Python3 is installed on every system but if you get errors on python-related files then double-check that this is indeed the case.

## License

Trident is released under Apache 2 license. In the source tree
there are two directories called "rdf3x" and "snap". The first directory
contains a modified version of the RDF3X engine written by <a
href="https://db.in.tum.de/~neumann/">Thomas Neumann</a>. The source code of
RDF3X was released on  google code but unfortunately that repository is no
longer available. The original version of RDF3X is released with Creative
Commons. In order to comply with the original license, also this modified
version should be considered released with the same license.  The other
directory ("snap") contains a modified version of the SNAP library developed at
Stanford (<a href="http://snap.stanford.edu/snap/index.html">Here</a> is the
link to the homepage of the project). I modified the code of SNAP in order to
be able to scale up to larger graphs. Since the original version of SNAP is
released with the BSD license, then also my modified version is released with
the same license. 
