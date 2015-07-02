$ErrorActionPreference = "Stop"

# System Config
$msys = $env:MSYS
if (!$msys) {throw "MSYS environment variable not set"}

$mingw_dir = "$msys\mingw64"

$env:Path += ";$mingw_dir\bin"

$packages = @("mingw-w64-x86_64-zeromq")

# ----------------

#####
echo "Installing Pacman Package Deps"
#####

$packages |`
  ForEach-Object {
    & $msys\usr\bin\pacman -S -q $_ --needed --noconfirm
  }

####
echo "Building with Stack"
& stack build --extra-include-dirs=$msys/mingw64/include --extra-lib-dirs=$msys/mingw64/lib
if (!$?) { throw "Build failed" }
& stack test --extra-include-dirs=$msys/mingw64/include --extra-lib-dirs=$msys/mingw64/lib
if (!$?) { throw "Build failed" }

echo "Done"
