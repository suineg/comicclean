Scripts here are all built with Cursor and not by a python expert. They are all tested on a real library full of archives with messy structures inside. 

Not every case is figured out but enough ones that I can see are taken care of. 

Be careful with your own data because only you care about it. 

double_page_fixing.py will take multiple different methods of listing a double page and replacing the filename with a dash between the page numbers so that readers properly sort them. It has not been tested for pages more than 999. 

You have the following switches available:

-a will run automatically and make all changes it feels like it should make without any confirmations
-d will do a dry run and show you all changes it feels like it should make but not change anything (so you can see if it chose correctly)
-o will output to a file that you specify all the changes it feels like it should make (this can be used with -a or -d)

You cannot use -a and -d at the same time of course.
