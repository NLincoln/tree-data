# tree-data
On-disk, btree-based database that offers strong serializability.

If you're familiar with MUMPS, it shares a similar core design. MUMPS does a lot of other things though that I didn't write.

## How do you offer strong serializability??

By making so that only one process can access the database file, and making every database operation - read and write - require `&mut`.
