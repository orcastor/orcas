
# Building with SQLCipher

To produce an encrypted build you need a SQLCipher/OpenSSL toolchain that matches the glibc version of your target device. Reusing `.so` files compiled on newer distros will fail with errors such as `GLIBC_2.34 not found`. The safe approach is to build SQLCipher locally on the target box.

## 1. Build SQLCipher from source

```bash
git clone https://github.com/sqlcipher/sqlcipher.git
cd sqlcipher

# Adjust --prefix as you like
CFLAGS="-DSQLITE_HAS_CODEC" \
LDFLAGS="-L/usr/lib/x86_64-linux-gnu" \
./configure \
  --enable-tempstore \
  --disable-tcl \
  --with-crypto-lib=openssl \
  --prefix=$HOME/sqlcipher-local

make -j$(nproc)
make install
```

If your OpenSSL headers/libs live outside the system default paths, append `-I/path/to/include` to `CFLAGS` and `-L/path/to/lib` to `LDFLAGS`.

## 2. (Optional) Build OpenSSL locally

```bash
git clone https://github.com/openssl/openssl.git
cd openssl
./Configure linux-x86_64 --prefix=$HOME/openssl-local
make -j$(nproc)
make install
```

Then point SQLCipher’s `CFLAGS/LDFLAGS` to `$HOME/openssl-local/{include,lib}`.

## 3. Copy libs next to the Go sources

Place the resulting `libsqlcipher.so` (and, if needed, `libcrypto.so`) under `core/` so they are available at build and run time. The repository ships a `core/sqlcipher_build.go` that, when built with the `sqlcipher` tag, links against libraries located in that directory and sets `rpath=$ORIGIN`.

```
/DATA/orcas/core/libsqlcipher.so
/DATA/orcas/core/libcrypto.so   # optional if you rely on system libcrypto
```

## 4. Build the Go binaries

```bash
cd /DATA/orcas
CGO_ENABLED=1 go build -tags sqlcipher ./cmd
```

At runtime the loader will pick the `.so` files from the `core/` folder thanks to the embedded rpath. If you prefer to use system-installed libraries instead, simply omit the copy step and rely on `pkg-config` by installing the distro’s `libsqlcipher-dev` package.


