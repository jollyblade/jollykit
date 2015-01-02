jollykit
========

Useful utility and design template classes for Java 8
Ideas, notes, suggestions welcome!

# ChunkProcessor: use case example: having large datasets loading them at once can use up all the memory, so it just takes a list of ids, referring to the data, splits
the list to smaller chunks, loads the data using the chunks, and processes them, after that aggregates the counts.

