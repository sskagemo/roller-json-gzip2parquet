# roller-json-gzip2parquet

Skript som laster ned json.gzip med totalbestand av roller fra Enhetsregisteret, og lagrer den mest relevante delen i en parquet-fil, etter å ha "forflatet" dataene for å gjøre det enklere å jobbe med videre.

Skriptet er nesten utelukkende laget med Claude Opus 4.5, gjennom bruk av Cline i VS Code 30. desember 2025. Se detaljer om arbeidet i plan.md

Claude anbefaler følgende for å redusere minneforbruket ytterligere:

"Option B: True streaming decompression (requires code change)
Implement proper streaming from HTTP response → gzip → ijson without buffering the compressed data. This would reduce peak memory to ~150-200 MB."

Work-around er å bruke muligheten til å lagre gzip til disk (```--save-json roller.json.gz```). Da skrives gzip-filen fortløpende til disk, og leses derfra (streaming). Normalt vil hele zip-filen bli holdt i minnet før den leses. Men filen utgjør ikke mer enn ca 125 MB.

Todo:
- endre default batchstørrelse til 50.000 eller 100.000
- lage default output-filnavn, basert på dato og klokkeslett for nedlasting
- oppdatere README med layout for parquet
- se om det er deler av parquet-fila som bør endres til categorical-datatype
- lage en skript-fil som inkluderer metadata om dependencies, slik at det blir helt stand-alone, for enkel deling