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
mage -v build:linuxARM64

Start the Grafana dev server.
docker compose up

Grafana should now be running with your plugin at:
http://localhost:3000

