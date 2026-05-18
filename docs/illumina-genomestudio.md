# Illumina GenomeStudio (GSGT Final Report) Support

How to add support for the Illumina **GenomeStudio Genotyping (GSGT) Final
Report** export — locally known as the *Carigenetics* format — to a system
that already understands flat SNP-array genotype files (e.g. the Dynamic DNA
"DDNA" plus-strand Build-38 export).

Read this if you are wiring a second array vendor into an existing
rsid-keyed genotype → VCF / allele-frequency pipeline.

---

## 1. What the format is

A GenomeStudio "Final Report" with a `[Header]` metadata block followed by a
`[Data]` block with a tab-separated column header and one row **per probe per
sample**:

```
[Header]
GSGT Version	2.0.5
Processing Date	12/23/2025 2:45 PM
Content		DTCbooster_20033558_A2.bpm
Num SNPs	710646
Total SNPs	710646
Num Samples	155
Total Samples	155
File 	1 of 155
[Data]
Sample ID	Sample Name	SNP Name	SNP	Chr	Position	Allele1 - Top	Allele2 - Top	Allele1 - Forward	Allele2 - Forward	Allele1 - Plus	Allele2 - Plus	Allele1 - Design	Allele2 - Design	Allele1 - AB	Allele2 - AB	Plus/Minus Strand
PC0001		1:103380393	[T/C]	1	102914837	G	G	G	G	G	G	C	C	B	B	-
```

Contrast with the DDNA format which is a single `#`-commented block then
`rsid  chromosome  position  genotype  gs  baf  lrr` — one clean rsid per row,
plus-strand, Build 38.

---

## 2. Quirks (the important part)

### 2.1 No genome build is declared
The `[Header]` has **no build line**. The build is implicit in the `.bpm`
manifest (`Content` field). Empirically the `Position` column is **GRCh38,
plus strand** — proven by joining a known same-individual DDNA file (explicit
GRCh38) on `chrom:pos` and getting **100.00% genotype concordance** over
682k SNPs. A GRCh37↔38 mismatch would have produced ~0%. **Trust the
`Position` column; do not trust the coordinate embedded in `SNP Name`.**

### 2.2 `SNP Name` is NOT an rsid
It is a probe id. Observed forms:

| Form | Example | Notes |
|---|---|---|
| plain rs | `rs11466023` | ~51% |
| prefixed/suffixed rs | `BOT-rs1135675`, `rs111647200_ilmndup1`, `GSA-rs61660502`, `seq-rs786202193` | ~45% — recover with `rs\d+` extraction |
| chr:pos | `1:103380393` | legacy coordinate, *not* GRCh38 |
| chr:pos + alleles | `1:159174749-C-T` | |
| CNV gene marker | `1:110228436_CNV_GSTM1` | inside a copy-number gene |
| MNV | `1:45332290_MNV` | multi-nucleotide |
| vendor/clinical | `DICER1-chr14-95596479`, `GALC:c.2002A>C`, `GA026708` | curated panel content |

Rule: **extract the first `rs\d+` substring**. ~96% of probes yield an rs id
this way. The rest have no rs id and must resolve by position (§4).

### 2.3 There is a separate `SNP` column — do not confuse it with the id
`SNP` = the probe's **design alleles**, e.g. `[T/C]`. After header
normalisation (`lowercase`, strip spaces/dashes/underscores) `SNP Name` →
`snpname` and `SNP` → `snp`. A generic alias map that treats `snp` as an
rsid alias will grab the design alleles instead of the probe id. Map the id
explicitly from `snpname`.

### 2.4 Five strand orientations — use **Plus**
Each call appears as `Allele1/2 - Top / Forward / Plus / Design / AB`.
Use **`Allele1 - Plus` / `Allele2 - Plus`**: plus strand aligns with the
GRCh38 reference and matches the DDNA plus-strand export (this is why the
concordance check hits 100%). The design `[A/G]` pair may be on the opposite
strand from Plus — handle strand-agnostically (§4.3).

### 2.5 No-call is `-` per allele
DDNA uses `--`; GenomeStudio uses a single `-` in each Plus allele column.
Map `-`/`-` → no-call (`--` / `./.`).

### 2.6 Indels are `I` / `D` in the Plus columns
Same convention as DDNA (`II`, `DD`, `ID`). They are not ACGT and will (and
should) fail SNP genotype→dosage; let them no-call through the existing path.

### 2.7 `Chr` / `Position` can be `0` — unmapped
~0.1% of probes (CNV markers, some indels) are unplaceable on GRCh38.
Skip rows where `Chr == 0` or `Position == 0`.

### 2.8 Duplicate probes for the same variant
`BOT-`, `BOT2-`, `GSA-`, `seq-`, `…_ilmndup1` are **replicate probes for the
same rs at the same `chrom:pos`**. They produce multiple rows for one variant
and frequently include a successful call alongside a no-call from a failed
replicate. These must be **merged** (§5).

### 2.9 Co-located *distinct* variants
A SNP probe and an indel probe (or two different rs) can share an exact
`chrom:pos` (e.g. a clinical indel beside a SNP in `CFTR`/`BRCA2`/`MSH2`).
These are **different variants and must stay separate** — do not collapse by
position alone.

### 2.10 CNV gene markers are low-trust
`_CNV_GSTM1` / `_CNV_GSTT1` probes tile copy-number genes. Many are
*paralogous-sequence variants* (fixed differences between gene paralogs, not
true polymorphisms) and genotype calls there are confounded by the common
whole-gene deletion. They resolve poorly on purpose (land in ambiguous /
unresolved) and should be tagged so they can be excluded from
allele-frequency.

---

## 3. Verification approach

Before trusting a new vendor's parsing, prove a **known same-individual**
file from the old format and the new format are genotype-concordant:

1. Convert the new file to the old flat format (or both to VCF).
2. Join on `chrom:pos`, compare sorted allele pairs over SNP sites where
   both have ACGT calls.
3. Expect **>99.9%**. (PC0001: 100.00% over 682,024 comparable SNPs, zero
   strand flips — confirming the Plus-column choice.)
4. The handful of mismatches at A/T or C/G sites are palindromic
   strand-ambiguity, not a parsing error.

Scripts: `scripts/carigenetics_to_ddna.py`, `scripts/diff_ddna.py`.

---

## 4. Adding support to an rsid-keyed pipeline

Assume the pipeline already resolves `rsid → (chrom,pos,ref,alt)` from a
reference DB and emits VCF/allele-freq. Three additions:

### 4.1 Parser
- Detect the format: a line equal to `[Header]` (case-insensitive).
- Skip everything until `[Data]`; ignore metadata lines between them.
- The first line after `[Data]` is the column header.
- `rsid` = first `rs\d+` in `SNP Name`; **allow it to be empty** (do not
  skip the row — it may resolve by position).
- `chrom` = `Chr`, `pos` = `Position` (skip if either is `0`).
- `genotype` = `Allele1 - Plus` + `Allele2 - Plus`; `-` → no-call.

### 4.2 Reference resolution with a position fallback
- rs-derivable probes (~96%) resolve through the existing rsid path.
- Non-rs probes have no rsid. Pre-resolve them once (§4.3) into a
  dedicated table keyed by `snp_name` and indexed by `(chromosome,
  position)`, then at convert time: if the rsid is empty or unresolved,
  look up `(chrom,pos)` in that table and use its `rsid/ref/alt`.

### 4.3 Resolving the non-rs markers (offline, one-off)
For each non-rs probe's GRCh38 `(chrom,pos)`, query a variant DB:

- **dbSNP VCF** works but NCBI throttles per-IP hard (~28 GB bulk ≈ 15–30 h;
  even remote tabix streaming ≈ 8 h). Use **Ensembl REST**
  (`/overlap/region/human/{chr}:{pos}-{pos}?feature=variation`, GRCh38,
  ~13 req/s → ~28k markers in ~40 min) instead.
- Accept only when **exactly one SNP rs id** sits at the exact position.
- **Design-pair check (strand-agnostic):** the probe's `[A/G]` design pair
  must equal the dbSNP `{ref,alt}` set *or its complement*. This rejects
  paralog / wrong-position hits a looser "observed allele is a subset"
  check would pass. Filter Ensembl `-` deletion alleles (length 1 but not a
  SNP).
- More than one variant, or design mismatch → **ambiguous log** (do not
  load — needs human decision).
- No SNP at the position → **unresolved log**.

PC0001 numbers (27,563 non-rs): 21,356 resolved (77.5%), 3,223 ambiguous,
2,984 unresolved.

### 4.4 De-duplicate resolved rsids before loading
A resolved rsid must be unique in the file:

- If it already exists among the file's own rs-derivable markers → drop
  (the native rs marker is canonical: `rsid_already_in_file`).
- If several non-rs probes resolve to the same rsid → drop all, log
  (`multiple_probes_same_rsid`).

PC0001: 21,356 → **18,276** unique loadable; 3,080 collisions logged.

Scripts: `scripts/extract_carigenetics_markers.py`,
`scripts/query_ensembl_nonrsids.py` (+ `query_dbsnp_nonrsids.py`
fallback), `scripts/variant_resolve.py`, `scripts/dedupe_resolved.py`.

---

## 5. Duplicate-probe merge (critical)

When multiple probe rows share a variant, collapse to **one** record.

**Merge key = `(chrom, pos, rsid)`** — NOT rsid alone.

- Same `(chrom,pos,rsid)` ⇒ real replicate probes (`BOT/BOT2/_ilmndup`).
- Different rsid at same `(chrom,pos)` ⇒ co-located distinct variants —
  stay separate (§2.9).
- Placeholder rsids (e.g. `.` in DDNA) at different positions ⇒ different
  key — *not* merged. **Keying by rsid alone silently collapses every `.`
  row into one and is a real regression** (cost us −4,188 DDNA rows once;
  the merge key must include chrom+pos).

Per group:

1. Drop no-calls (a no-call is missing data, not a contradiction).
2. 0 calls left → emit one no-call record.
3. All remaining calls agree → emit that genotype (this recovers
   call+no-call replicate pairs).
4. Calls disagree → emit **no-call**, flag `INFO=CONFLICT`, write a
   `dup_conflict` log line. **Do not auto-pick** — disagreements are real
   quality flags (strand-ambiguous flips, discordant replicates).

PC0001 (7,153 multi-probe rsids): 93.7% all-agree, 1.9% call+no-call
recovered, 0.3% all-no-call, **4.1% genuine disagreement → logged**.

---

## 6. Schema

Keep new-source provenance separate from the trusted core reference:

```sql
CREATE TABLE grch38_non_rsids (
    snp_name   TEXT PRIMARY KEY,          -- original "SNP Name" probe id
    rsid       INTEGER NOT NULL,          -- resolved via Ensembl/dbSNP
    chromosome TEXT NOT NULL,
    position   INTEGER NOT NULL,
    reference  TEXT NOT NULL,
    alternates TEXT NOT NULL,
    source     TEXT NOT NULL DEFAULT 'ensembl_grch38',
    note       TEXT
);
CREATE INDEX idx_grch38_non_rsids_pos  ON grch38_non_rsids(chromosome, position);
CREATE INDEX idx_grch38_non_rsids_rsid ON grch38_non_rsids(rsid);
```

Resolution priority for a position lookup: user overrides → this table →
base reference. Keeping these rows tagged (`source`) lets you exclude or
re-resolve the lower-confidence non-rs / CNV markers later without touching
the core.

---

## 7. End-to-end (this codebase)

```bash
BVS=./cli/target/debug/bvs ; DB=data/genostats.sqlite

# one-off: extract, resolve, dedupe, load the non-rs markers
python scripts/extract_carigenetics_markers.py "<file>.txt" --outdir out
python scripts/query_ensembl_nonrsids.py "out/<stem>.nonrsids.tsv"
python scripts/dedupe_resolved.py "out/<stem>.nonrsids.resolved.csv" \
       "out/<stem>.rsids.tsv"
"$BVS" load-non-rsids --sqlite "$DB" \
       --lookup "out/<stem>.nonrsids.resolved.dedup.csv"

# routine: the parser auto-detects [Header]; rsid path + position fallback
"$BVS" genotype-to-vcf -i "<file>.txt" --sqlite "$DB" --output out.vcf
"$BVS" emit-long -i "<file>.txt" --sqlite "$DB" --output out.bvlr
"$BVS" synthetic --format illumina --output synth.txt   # ddna is default
```

Validated on PC0001: Carigenetics → VCF 690,908 rows (**98.6% coverage**),
0 true duplicate loci, co-located SNP+indel preserved, 287 conflicts
logged; DDNA regression unchanged (692,054 rows); allele-frequency pipeline
(`emit-long` → `aggregate-long`) works on both formats.

---

## 8. Gotchas checklist

- [ ] Trust `Position`, not the `SNP Name` coordinate.
- [ ] Map id from `SNP Name`, never the `SNP` (design) column.
- [ ] Use the **Plus** allele columns.
- [ ] `-` is no-call; `I`/`D` are indels (let them no-call).
- [ ] Skip `Chr`/`Position == 0`.
- [ ] Allow empty rsid → position fallback.
- [ ] Merge duplicates by `(chrom,pos,rsid)`, never rsid alone.
- [ ] Disagreeing replicates → no-call + conflict log, never auto-pick.
- [ ] Tag resolved non-rs / CNV markers by source; keep them out of the
      trusted core table.
- [ ] Don't bulk-download dbSNP from NCBI; use Ensembl REST.
