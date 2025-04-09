In order to avoid wearing out your SSD, tsdbtest runs on a RAM disk instead of
a real disk.  You can create a RAM disk from the macOS terminal as follows:

diskutil erasevolume APFS 'ram_disk' `hdiutil attach -nobrowse -nomount ram://12582912`

The number at the end of the command is the size of the requested RAM disk in
512-byte blocks.  In its current state, tsdbtest requires around 3.5G of
storage.  The command above allocates a 6G RAM disk which is more than enough.
You will need to delete the database between tsdbtest runs.
