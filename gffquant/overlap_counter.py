import os
import time
import sys
from collections import Counter
from itertools import product

import numpy as np
import pandas as pd

"""
normalizeCounts nmethod counts sizes
    | nmethod `elem` [NMScaled, NMFpkm] = do
        -- count vectors always include a -1 at this point (it is
        -- ignored in output if the user does not request it, but is
        -- always computed). Thus, we compute the sum without it and do
        -- not normalize it later:
        let totalCounts v = withVector v (VU.sum . VU.tail)
        initial <- totalCounts counts
        normalizeCounts NMNormed counts sizes
        afternorm <- totalCounts counts
        let factor
                | nmethod == NMScaled = initial / afternorm
                | otherwise = 1.0e9 / initial --- 1e6 [million fragments] * 1e3 [kilo basepairs] = 1e9
        liftIO $ forM_ [1.. VUM.length counts - 1] (VUM.unsafeModify counts (* factor))
"""
class OverlapCounter:
    @staticmethod
    def _generate_counter_columns(stranded=False, feature=False):
        transforms = ("raw", "normed") + (("scaled",) if feature else tuple())
        base_columns = tuple(product(("unique", "ambiguous"), transforms))
        if stranded:
            columns = list()
            for strand in (None, False, True):
                for col in base_columns:
                    columns.append(col + (strand, ))
            return columns
        return base_columns
                
    def _get_annotation_function(self, annotation_mode):
        annotation_modes = {
            "counts": self._iterate_counts,
            "database": self._iterate_database,
            "bedcounts": self._iterate_bedcounts
        }
        try:
            return annotation_modes[annotation_mode]
        except KeyError:
             raise ValueError(f"Unknown annotation_mode {itermode}")

    def _calculate_row(self, rid, aln, strand):
        start, end, rev_strand = aln
        region_length = end - start + 1
        row = self.counts.loc[rid, :]
        if self.strand_specific:
            row[0] = row[6] + row[12]
            row[3] = row[9] + row[15]
        for i in range(1, len(self.cols), 3):
            row[i] = row[i-1] / region_length
        return row 

    #def _distribute_feature_counts(self, bins, counts, region_annotation, total_counts, feature_count_sums):
    def _distribute_feature_counts(self, region, features, total_counts, feature_count_sums):
        rid, start, end, rev_strand = region
        for feature_cat, feature_subcats in features:
            for i, fsubcat in enumerate(feature_subcats, start=1):
                row_key = region
                col_key = ("unique", "raw") + ((rev_strand,) if self.strand_specific else tuple())
                counts = self.counts.loc[row_key, col_key]
                fcounts = np.zeros(len(self.fcols))
                k = 0
                for j, c in enumerate(counts, start=1):
                    if j % 3 == 0:
                        k += 1
                    fcounts[k] = c
                    k += 1 
                try:
                    
                
                    new_row, update_col = np.zeros(len(self.cols)), 0
                    if self.strand_specific:
                        update_col = 12 if rev_strand else 6
                    new_row[update_col] = 1
                    self.counts = self.counts.append(
                        pd.DataFrame(data=[new_row], index=[row_key], columns=self.counts.columns)
                    )






            
        for ftype, ftype_counts in region_annotation:
            for i, ft_ct in enumerate(ftype_counts, start=1):
                fcounts = self.featcounts.setdefault(ftype, dict()).setdefault(ft_ct, np.zeros(bins))
                total_fcounts = feature_count_sums.setdefault(ftype, np.zeros(4))
                fcounts += counts

            inc = counts[:4] * i
            total_counts += inc
            total_fcounts += inc

        return total_counts, feature_count_sums
    




    def _iterate_counts(self, bins, refdata):
        total_counts, feature_count_sums = np.zeros(4), dict()

        current_rid = None
        for rid, start, end, rev_strand in self.counts.index:
            if current_rid != rid:
                ref, rlen = refdata.get(rid)
            (_, strand), (_, gene_id), *features = self.db.get_data(ref, start, end)
            counts = self._calculate_row(rid, (start, end, rev_strand), strand)
            total_counts, feature_count_sums = self._distribute_feature_counts(bins, counts, features, total_counts, feature_count_sums)

        return total_counts, feature_count_sums
            
    def _iterate_database(self):
        ...
    def _iterate_bedcounts(self):
        ...

    @staticmethod
    def normalise_counts(counts, feature_len, scaling_factor):
        '''Returns raw, length-normalised, and scaled feature counts.'''
        normalised = counts / feature_len
        scaled = normalised * scaling_factor
        return counts, normalised, scaled

    def __init__(self, nrefs=0, db=None, do_overlap_detection=True, strand_specific=False):
        self.db = db
        self.do_overlap_detection = do_overlap_detection
        self.strand_specific = strand_specific
        self.cols = OverlapCounter._generate_counter_columns(stranded=strand_specific)
        self.counts = pd.DataFrame(data=None, columns=self.cols, dtype=np.float64)
        self.seqcounts = pd.DataFrame(
            data=[[0] * len(self.cols)] * nrefs, index=range(nrefs), columns=self.cols, dtype=np.float64
        ) 
        self.fcols = OverlapCounter._generate_counter_columns(stranded=strand_specific, feature=True)
        self.featcounts = pd.DataFrame(data=None, columns=self.fcols, dtype=np.float64)
        self.unannotated = 0

    def update_unique_counts(self, rid, aln=None, rev_strand=False):
        if aln:
            overlaps = [0] #self.db.get_overlaps(*aln)
            self.unannotated += min(1, len(overlaps))
            for ovl in overlaps:
                #row_key = (rid, ovl.begin, ovl.end, rev_strand)
                row_key = (4, 1, 2, None)
                col_key = ("unique", "raw") + ((rev_strand,) if self.strand_specific else tuple())
                print(col_key)
                try:
                    self.counts.at[row_key, col_key] += 1
                except KeyError:
                    new_row, update_col = np.zeros(len(self.cols)), 0
                    if self.strand_specific:
                        update_col = 12 if rev_strand else 6
                    new_row[update_col] = 1
                    self.counts = self.counts.append(
                        pd.DataFrame(data=[new_row], index=[row_key], columns=self.counts.columns)
                    )

        self.seqcounts.at[rid, col_key] += 1

    def annotate_counts(self, refdata, annotation_mode="counts"):
        print("Processing counts ...", flush=True)
        t0 = time.time()
        bins = 12 if self.strand_specific else 4
        default_scaling_factor = 0

        annotation_f = self._get_annotation_function(annotation_mode)
        total_counts, feature_count_sums = annotation_f(bins, refdata)

        # calculate the scaling factors
        total, total_normed, total_ambi, total_ambi_normed = total_counts

        for ftype, counts in feature_count_sums.items():
            total, total_normed, total_ambi, total_ambi_normed = counts
            self.feature_scaling_factors[ftype] = (
                (total / total_normed) if total_normed else default_scaling_factor,
                (total_ambi / total_ambi_normed) if total_ambi_normed else default_scaling_factor
            )

        t1 = time.time()
        print("Processed counts in {n_seconds}s.".format(n_seconds=t1-t0), flush=True)



                

class xOverlapCounter(dict):
    def __init__(self, out_prefix, db, do_overlap_detection=True, strand_specific=False):
        self.out_prefix = out_prefix
        self.db = db
        self.seqcounts = Counter()
        self.ambig_seqcounts = Counter()
        self.featcounts = dict()
        self.unannotated_reads = 0
        self.ambig_counts = dict()
        self.has_ambig_counts = False
        self.feature_scaling_factors = dict()
        self.do_overlap_detection = do_overlap_detection
        self.strand_specific = strand_specific


    def _iterate_database(self, bins, bam, strand_specific):
        total_counts, feature_count_sums = np.zeros(4), dict()

        for ref, region_annotation in self.db.iterate():
            rid = bam.revlookup_reference(ref)
            if rid is not None:
                _, region_length = bam.get_reference(rid)
                counts = np.zeros(bins)

                if strand_specific:
                    counts[0] = counts[1] = self.seqcounts[(rid, True)] + self.seqcounts[(rid, False)]
                    counts[1] /= region_length
                    counts[2] = counts[3] = counts[0] + self.ambig_seqcounts[(rid, True)] + self.ambig_seqcounts[(rid, False)]
                    counts[3] /= region_length
                    counts[4] = counts[5] = self.seqcounts[(rid, True)]
                    counts[5] /= region_length
                    counts[6] = counts[7] = counts[4] + self.ambig_seqcounts[(rid, True)]
                    counts[7] /= region_length
                    counts[8] = counts[9] = self.seqcounts[(rid, False)]
                    counts[9] /= region_length
                    counts[10] = counts[11] = counts[8] + self.ambig_seqcounts[(rid, False)]
                    counts[11] /= region_length
                else:
                    counts[0] = counts[1] = self.seqcounts[rid]
                    counts[1] /= region_length
                    counts[2] = counts[3] = counts[0] + self.ambig_seqcounts[rid]
                    counts[3] /= region_length

                total_counts, feature_count_sums = self._distribute_feature_counts(bins, counts, region_annotation[1:], total_counts, feature_count_sums)

        return total_counts, feature_count_sums


    def _iterate_bedcounts(self, bins, feature_lengths, strand_specific):
        total_counts, feature_count_sums = np.zeros(4), dict()

        for rid in set(self.seqcounts).union(self.ambig_seqcounts):
            region_length = feature_lengths.get(rid)
            feature = rid.split("::")[-1]

            if region_length is None:
                raise ValueError(f"Cannot determine length of reference {rid}.")
            counts = np.zeros(bins)

            if strand_specific:
                counts[0] = counts[1] = self.seqcounts[(rid, True)] + self.seqcounts[(rid, False)]
                counts[1] /= region_length
                counts[2] = counts[3] = counts[0] + self.ambig_seqcounts[(rid, True)] + self.ambig_seqcounts[(rid, False)]
                counts[3] /= region_length
                counts[4] = counts[5] = self.seqcounts[(rid, True)]
                counts[5] /= region_length
                counts[6] = counts[7] = counts[4] + self.ambig_seqcounts[(rid, True)]
                counts[7] /= region_length
                counts[8] = counts[9] = self.seqcounts[(rid, False)]
                counts[9] /= region_length
                counts[10] = counts[11] = counts[8] + self.ambig_seqcounts[(rid, False)]
                counts[11] /= region_length
            else:
                counts[0] = counts[1] = self.seqcounts[rid]
                counts[1] /= region_length
                counts[2] = counts[3] = counts[0] + self.ambig_seqcounts[rid]
                counts[3] /= region_length

            total_counts, feature_count_sums = self._distribute_feature_counts(bins, counts, [("feature", (feature,))], total_counts, feature_count_sums)

        return total_counts, feature_count_sums















    def _iterate_counts(self, bins, bam, strand_specific):
        total_counts, feature_count_sums = np.zeros(4), dict()

        for rid in set(self.keys()).union(self.ambig_counts):
            ref = bam.get_reference(rid)[0]
            for start, end, rev_strand in set(self.get(rid, set())).union(self.ambig_counts.get(rid, set())):
                # region_annotation is a tuple of key-value pairs: (strand, func_category1: subcategories, func_category2: subcategories, ...)
                # the first is the strand, the second is the gene id, the rest are the features
                region_annotation = self.db.get_data(ref, start, end)
                counts = self._compute_count_vector(bins, rid, (start, end, rev_strand), region_annotation[0][1], strand_specific)
                # how to extract the gene counts in genome mode?
                # self.seqcounts[(region_annotation[1][1], rev_strand)] += counts[0]
                # self.ambig_seqcounts[(region_annotation[1][1], rev_strand)] += counts[2] - counts[0]

                # distribute the counts to the associated functional (sub-)categories
                total_counts, feature_count_sums = self._distribute_feature_counts(bins, counts, region_annotation[2:], total_counts, feature_count_sums)

        return total_counts, feature_count_sums




    def update_ambiguous_counts(self, hits, n_aln, unannotated=0, bam=None, feat_distmode="all1"):
        self.has_ambig_counts = True
        self.unannotated_reads += unannotated
        if self.strand_specific and not self.do_overlap_detection:
            n_total = sum(self.seqcounts[(rid, True)] + self.seqcounts[(rid, False)] for rid in hits)
        else:
            n_total = sum(self.seqcounts[rid] for rid in hits)
        for rid, regions in hits.items():
            if self.do_overlap_detection:
                for start, end, rev_str in regions:
                    self.ambig_counts.setdefault(rid, Counter())[(start, end, rev_str)] += (1 / n_aln) if feat_distmode == "1overN" else 1
#                reg_count = self.ambig_counts.setdefault(rid, Counter())
#
#                if feat_distmode == "all1":
#                    increment = 1
#                elif feat_distmode == "1overN":
#                    increment = 1 / n_aln
#                else:
#                    uniq_counts = self.get((start, end, True), 0) + self.get((start, end, False), 0)
#                    if n_total and uniq_counts:
#                        increment = uniq_counts /
#
#                reg_count[(start, end, rev_str)] += increment

#290             hits.setdefault(rid, set()).add((start, end, SamFlags.is_reverse_strand(flag)))

                if n_total and self.seqcounts[rid]:
                    self.ambig_seqcounts[rid] += self.seqcounts[rid] / n_total * len(hits)
                else:
                    self.ambig_seqcounts[rid] += 1 / len(hits)
            else:
                start, end, rev_str = list(regions)[0]
                key = (rid, rev_str) if self.strand_specific else rid
                self.ambig_seqcounts[key] += (1 / n_aln) if feat_distmode == "1overN" else 1

    @staticmethod
    def calculate_seqcount_scaling_factor(counts, bam):
        raw_total = sum(counts.values())
        normed_total = sum(count / bam.get_reference(rid)[1] for rid, count in counts.items())
        return raw_total / normed_total
    @staticmethod
    def calculate_feature_scaling_factor(counts, include_ambig=False):
        raw_total, normed_total = 0, 0
        for raw, norm, raw_ambi, norm_ambi, *_ in counts.values():
            raw_total += raw
            normed_total += norm
            if include_ambig:
                raw_total += raw_ambi
                normed_total += norm_ambi
        return raw_total / normed_total

    def dump_counts(self, bam=None):

        COUNT_HEADER_ELEMENTS = ["raw", "lnorm", "scaled"]
        SEQ_COUNT_HEADER = ["seqid_int", "seqid", "length"] + COUNT_HEADER_ELEMENTS
        counts_template = "{:.5f}\t{:.5f}\t{:.5f}"

        print("Dumping overlap counters...", flush=True)
        print("Has ambiguous counts:", self.has_ambig_counts, flush=True)
        if self.strand_specific and not self.do_overlap_detection:
             _seqcounts = Counter()
             for (rid, rev_strand), count in self.seqcounts.items():
                 _seqcounts[rid] += count
             self.seqcounts = _seqcounts
             if self.has_ambig_counts:
                 _seqcounts = Counter()
                 for (rid, rev_strand), count in self.ambig_seqcounts.items():
                     _seqcounts[rid] += count
                 self.ambig_seqcounts = _seqcounts

        if bam:
            with open("{prefix}.seqname.uniq.txt".format(prefix=self.out_prefix), "w") as seq_out:
                print(*SEQ_COUNT_HEADER, sep="\t", flush=True, file=seq_out)
                if sum(self.seqcounts.values()):
                    seqcount_scaling_factor = OverlapCounter.calculate_seqcount_scaling_factor(self.seqcounts, bam)
                    for rid, count in self.seqcounts.items():
                        seq_id, seq_len = bam.get_reference(rid)
                        print(rid, seq_id, seq_len, count, "{:.5f}\t{:.5f}".format(*OverlapCounter.normalise_counts(count, seq_len, seqcount_scaling_factor)[1:]), flush=True, sep="\t", file=seq_out)

        with open("{prefix}.feature_counts.txt".format(prefix=self.out_prefix), "w") as feat_out:

            header = ["subfeature"]
            header.extend("uniq_{}".format(element) for element in COUNT_HEADER_ELEMENTS)
            if self.has_ambig_counts:
                header.extend("combined_{}".format(element) for element in COUNT_HEADER_ELEMENTS)
            if self.strand_specific:
                for strand in ("ss", "as"):
                    header.extend("uniq_{}_{}".format(element, strand) for element in COUNT_HEADER_ELEMENTS)
                    if self.has_ambig_counts:
                        header.extend("combined_{}_{}".format(element, strand) for element in COUNT_HEADER_ELEMENTS)
            print(*header, sep="\t", file=feat_out, flush=True)

            print("unannotated", self.unannotated_reads, sep="\t", file=feat_out, flush=True)
            for ftype, counts in sorted(self.featcounts.items()):
                print("#{}".format(ftype), file=feat_out, flush=True)
                #feature_scaling_factor = OverlapCounter.calculate_feature_scaling_factor(counts)
                #feature_scaling_factor_ambig = OverlapCounter.calculate_feature_scaling_factor(counts, include_ambig=True)
                scaling_factor, scaling_factor_ambi = self.feature_scaling_factors[ftype]

                for subf, sf_counts in sorted(counts.items()):
                    # first batch: unique
                    out_row = list(sf_counts[:2])
                    out_row.append(out_row[-1] * scaling_factor)
                    # next batch: ambiguous (if exist)
                    if self.has_ambig_counts:
                        out_row.extend(sf_counts[2:4])
                        out_row.append(out_row[-1] * scaling_factor_ambi)
                    # next batch: sense-strand unique
                    if self.strand_specific:
                        out_row.extend(sf_counts[4:6])
                        out_row.append(out_row[-1] * scaling_factor)
                        # next batch: sense-strand ambiguous
                        if self.has_ambig_counts:
                            out_row.extend(sf_counts[6:8])
                            out_row.append(out_row[-1] * scaling_factor_ambi)
                        # next batch antisense-strand unique
                        out_row.extend(sf_counts[8:10])
                        out_row.append(out_row[-1] * scaling_factor)
                        # next batch: antisense-strand ambiguous
                        if self.has_ambig_counts:
                            out_row.extend(sf_counts[10:12])
                            out_row.append(out_row[-1] * scaling_factor_ambi)

                    print(subf, out_row[0], *("{:.5f}".format(c) for c in out_row[1:]), flush=True, sep="\t", file=feat_out)

        if bam and self.ambig_seqcounts:
                with open("{prefix}.seqname.dist1.txt".format(prefix=self.out_prefix), "w") as seq_out:
                    print(*SEQ_COUNT_HEADER, sep="\t", flush=True, file=seq_out)
                    self.seqcounts.update(self.ambig_seqcounts)
                    seqcount_scaling_factor = OverlapCounter.calculate_seqcount_scaling_factor(self.seqcounts, bam)
                    for rid, count in self.seqcounts.items():
                        seq_id, seq_len = bam.get_reference(rid)
                        print(rid, seq_id, seq_len, counts_template.format(*OverlapCounter.normalise_counts(count, seq_len, seqcount_scaling_factor)), flush=True, sep="\t", file=seq_out)
