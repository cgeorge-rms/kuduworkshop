# kuduworkshop
kudu workshop repository

To run any of this you need to set the binDir environment variable so the code knows where kudu binaries are.
This project is only setup to run on OSX 10.11 (El Capitan)

Before you run anything set a global environment variable -DbinDir=$MODULE_DIR$/kudubinary in vm options
You can do this by going to Run > Edit Configurations
Expand the defaults
select Junit and add -DbinDir=$MODULE_DIR$/kudubinary to the end of vmoptions

you'll also need to run brew install openssl
