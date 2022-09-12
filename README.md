# indices

This directory contains a command line program for calculating climate indices
such as those used by the [ETCCDI](http://etccdi.pacificclimate.org/list_27_indices.shtml) and
[Climdex](https://www.climdex.org/) projects.

The program makes use of the [icclim](https://icclim.readthedocs.io/en/stable/) library.
Detailed descriptions of each index are available in the
icclim [API](https://icclim.readthedocs.io/en/stable/references/ecad_functions_api.html#module-icclim._generated_api) and
and an associated [ATBD](https://www.ecad.eu/documents/atbd.pdf).

Running the script at the command line with the `-h` option explains the user options:

```bash
$ python run_icclim.py -h
```


