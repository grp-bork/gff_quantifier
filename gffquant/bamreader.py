import sys
import time
import gzip
import struct

from collections import Counter

class SamFlags:
	PAIRED = 0x1
	MATE_UNMAPPED = 0x8
	REVERSE = 0x10
	FIRST_IN_PAIR = 0x40
	SECOND_IN_PAIR = 0x80
	SECONDARY_ALIGNMENT = 0x100
	SUPPLEMENTARY_ALIGNMENT = 0x800

	@staticmethod
	def is_reverse_strand(flag):
		return bool(flag & SamFlags.READ_REVERSE)


class BamAlignment:
	CIGAR_OPS = "MIDNSHP=X"
	@staticmethod
	def parse_cigar(cigar_ops):
		# op_len << 4 | op
		return [(c >> 4, c & 0xf) for c in cigar_ops]
	@staticmethod
	def show_cigar(cigar):
		return "".join(["{oplen}{op}".format(oplen=c[0], op=BamAlignment.CIGAR_OPS[c[1]]) for c in cigar])
	@staticmethod
	def calculate_coordinates(start, cigar):
		# MIDNSHP=X
		# 012345678; 02378 consume reference: 0000 0010 0011 0111 1000
		REF_CONSUMERS = {0, 2, 3, 7, 8}
		return start + sum(oplen for oplen, op in cigar if op in REF_CONSUMERS)
	def shorten(self):
		return (aln.qname, aln.rid, aln.start, aln.end)
	def is_ambiguous(self):
		return self.mapq == 0
	def is_primary(self):
		return not bool(self.flag & SamFlags.SECONDARY_ALIGNMENT)
	def is_supplementary(self):
		return bool(self.flag & SamFlags.SUPPLEMENTARY_ALIGNMENT
	def is_unique(self):
		return self.mapq != 0
	def is_reverse(self):
		return bool(self.flag & SamFlags.REVERSE)
	def is_paired(self):
		return bool(self.flag & SamFlags.PAIRED)
	def __init__(self, qname=None, flag=None, rid=None, pos=None,
				 mapq=None, cigar=None, rnext=None, pnext=None,
				 tlen=None, len_seq=None, tags=None):
		self.qname = qname
		self.flag = flag
		self.rid = rid
		self.cigar = BamAlignment.parse_cigar(cigar)
		self.start = pos
		self.end = BamAlignment.calculate_coordinates(self.start, self.cigar)
		self.mapq = mapq
		self.rnext = rnext
		self.pnext = pnext
		self.tlen = tlen
		self.len_seq = len_seq
		self.tags = tags
	def get_hash(self):
		import hashlib
		md5 = hashlib.md5()
		md5.update(self.qname)
		return int(md5.hexdigest(), 16)
	def __str__(self):
		return "{rid}:{rstart}-{rend} ({cigar};{flag};{mapq};{tlen}) {rnext}:{pnext}".format(
			rid=self.rid, rstart=self.start, rend=self.end, cigar=BamAlignment.show_cigar(self.cigar), flag=self.flag,
			mapq=self.mapq, tlen=self.tlen, rnext=self.rnext, pnext=self.pnext
		 )

class BamFile:
	def __init__(self, fn, large_header=False):
		self._references = list()
		self._file = gzip.open(fn, "rb")
		self._fpos = 0
		self._data_block_start = 0
		self._read_header(large_header=large_header)

	def _read_header(self, large_header=False):
		try:
			magic = "".join(map(bytes.decode, struct.unpack("cccc", self._file.read(4))))
		except:
			raise ValueError("Could not infer file type.")
		if not magic.startswith("BAM"):
			raise ValueError("Not a valid bam file.")
		len_header = struct.unpack("I", self._file.read(4))[0]
		if len_header:
			# since we're quite close to the start, we don't lose much by seeking in bgzip'd file
			# obviously, if the header is to be retained, need to handle differently
			self._file.seek(len_header, 1)
			#bufsize = 102400 # 100Mb buffer
			#if large_header and len_header >= bufsize:
			#	for offset in range(0, len_header, bufsize):
			#		_ = self._file.read(min(bufsize, len_header - offset))
			#
			##>>> for i in range(0,5193,1024):
			##...     print(i, min(1024, 5193-i))
			#else:
			#	# header_str = struct.unpack("c" * len_header, self._file.read(len_header))
			#	_ = self._file.read(len_header)
		n_ref = struct.unpack("I", self._file.read(4))[0]
		self._fpos += 4 + 4 + len_header + 4
		for i in range(n_ref):
			len_rname = struct.unpack("I", self._file.read(4))[0]
			# rname = "".join(map(bytes.decode, struct.unpack("c" * len_rname, self._file.read(len_rname))[:-1]))
			rname = self._file.read(len_rname)[:-1] # not decoding here anymore, this saves a couple bytes
			len_ref = struct.unpack("I", self._file.read(4))[0]
			self._references.append((rname, len_ref))
			self._fpos += 4 + len_rname + 4
		self._data_block_start = self._fpos

	def get_reference(self, rid):
		rname, rlen = self._references[rid]
		return rname.decode(), rlen
	def n_references(self):
		return len(self._references)
	def get_position(self):
		return self._fpos
	def rewind(self):
		self._fpos = self._data_block_start
		self._file.seek(self._data_block_start)
	def seek(self, pos):
		if pos < self._data_block_start:
			raise ValueError("Position {pos} seeks back into the header (data_start={data_start}).".format(pos=pos, data_start=self._data_block_start))
		self._file.seek(pos)
		self._fpos = pos
	def get_alignments(self, required_flags=None, disallowed_flags=None, allow_unique=True, allow_multiple=True):
		aln_count = 1
		if not allow_multiple and not allow_unique:
			raise ValueError("Either allow_multiple or allow_unique needs to be True.")

		while True:
			try:
				aln_size = struct.unpack("I", self._file.read(4))[0]
			except struct.error:
				break

			rid, pos, len_rname, mapq, bai_bin, n_cigar_ops, flag, len_seq, next_rid, next_pos, tlen = struct.unpack(
				"iiBBHHHIiii",
				self._file.read(32)
			)

			qname = self._file.read(len_rname) # qname
			# qname = struct.unpack("{size}s".format(size=len_rname), self._file.read(len_rname))[0].decode().rstrip("\x00") # qname
			cigar = struct.unpack("I" * n_cigar_ops, self._file.read(4 * n_cigar_ops))
			self._file.read((len_seq + 1) // 2) # encoded read sequence
			self._file.read(len_seq) # quals

			total_read = 32 + len_rname + 4 * n_cigar_ops + (len_seq + 1) // 2 + len_seq
			tags = self._file.read(aln_size - total_read) # get the tags
			self._fpos += 4 + aln_size
			# print(*map(bytes.decode, struct.unpack("c"*len(tags), tags)))

			flag_check = (required_flags is None or (flag & required_flags)) and (disallowed_flags is None or not (flag & disallowed_flags)) 
			is_unique = mapq != 0
			atype_check = (is_unique and allow_unique) or (not is_unique and allow_multiple)

			if flag_check and atype_check:
				yield aln_count, BamAlignment(qname, flag, rid, pos, mapq, cigar, next_rid, next_pos, tlen, len_seq, tags)
			aln_count += 1

	@staticmethod
	def calculate_fragment_borders(start1, end1, start2, end2):
		'''
		Returns the minimum and maximum coordinates of a read pair.
		'''
		coords = sorted((start1, end1, start2, end2))
		return coords[0], coords[-1]


if __name__ == "__main__":
	bam = BamFile(sys.argv[1])
	for aln in bam.get_alignments():
		print(aln)
