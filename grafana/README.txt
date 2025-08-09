How to do Grafana plugins
=========================

Install instructions for Node 22 were found in the description of this YouTube
video:
https://www.youtube.com/watch?v=AARrATeVEQY

Install Node 22
===============
Start here to get Node 22:
https://nodejs.org/en/download

This is the summary of commands as of 14 Mar 2025:
curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.40.2/install.sh | bash
\. "$HOME/.nvm/nvm.sh"
nvm install 22
node -v # Should print "v22.14.0".
nvm current # Should print "v22.14.0".
npm -v # Should print "10.9.2".


For a brand new plugin
======================
Instructions here:
https://grafana.com/developers/plugin-tools/tutorials/build-a-data-source-plugin

Summary:
npx @grafana/create-plugin@latest
cd <your-plugin>


For working on an existing plugin
=================================
Update the packages:
npm install

Run the background script that watches for changes to the source tree:
npm run dev

Build the backend Go source:
# mage -v build:linuxARM64
mage -v build:linux

Start the Grafana dev server.
docker compose up

Grafana should now be running with your plugin at:
http://localhost:3000


To package and sign a plugin
============================
You will need to have an Access Policy Token tied to your Grafana Cloud
account.  Log in to grafana.com and click "My Account".  In the left sidebar
will be an entry called "Access Policies".  Click that and generate an access
policy token.  The realm should be your account or organization name (select
the one that says "all stacks").  The scope should be "plugins:write".  Create
the token and save the token contents somewhere (be careful since you won't be
able to access this again from grafana.com).

First, make sure to build the plugin:

npm run build
mage -v build:linux
# or: mage -v build:linuxARM64

Next, sign it:

export GRAFANA_ACCESS_POLICY_TOKEN=<YOUR_ACCESS_POLICY_TOKEN>
npx @grafana/sign-plugin@latest --rootUrls https://my.server.com:3000

Note: for the root URL, you should specify the full path, as it appears in
your browser's address bar, including port number if non-standard.

The dist/ subdirectory has now been signed, so now we can package the plugin:

mv dist/ tgree-simpletsdb-datasource
zip tgree-simpletsdb-datasource-1.0.0.zip tgree-simpletsdb-datasource -r

Finally, unzip a copy of the plugin into your plugins directory of your
Grafana install.  This will typically be in /var/lib/grafana/plugins.  Make
sure that the plugin is owned by grafana:grafana.

Restart Grafana and your plugin is now available for use.
