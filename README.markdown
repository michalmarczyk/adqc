# ADQC: Adaptive Distributed Query Compiler

This is a reimplementation of the
[OGSA-DAI](http://www.ogsadai.org.uk/) query compiler in Clojure.

Work on this project up to 16th August 2010 was done as part of the
Google Summer of Code 2010, by Michał Marczyk, under the supervision
of Bartosz Dobrzelecki.

For future work on the project, see the [Assembla
space](http://assembla.com/spaces/adqc) or the [GitHub development
repository](http://github.com/michalmarczyk/adqc).

# Building

You will need [Leiningen](http://github.com/technomancy/leiningen), a
popular Clojure build tool.  You will also need the jars from the
OGSA-DAI distribution; these are available from
[SourceForge](http://sourceforge.net/projects/ogsa-dai/files/).  You
can either install them into your local Maven 2 repository and have
Leiningen pull them into the project directory or place them in `lib/`
by hand.

# Licence

Copyright (C) 2010 The University of Edinburgh
Copyright (C) 2010 Michał Marczyk

Distributed under the terms of the [Apache License, Version
2.0](http://www.apache.org/licenses/LICENSE-2.0.html).
