import time
from apscheduler.schedulers.blocking import BlockingScheduler
sys.path.append("/home/limbo4/parser") # change this!
from limbo_parser.javad.greis import CODE_POS_VEL, CODE_SATIND
from limbo_parser.javad.greis import CODE_ELEVATION, CODE_AZIMUTH
from limbo_parser.javad.greis import _1r, _2r, _1p, _2p, rc
from limbo_parser.javad.greis import get_struct
from limbo_parser.javad.jps_stat import JPSStatistics

CODES = [CODE_POS_VEL, CODE_SATIND, CODE_ELEVATION, CODE_AZIMUTH,_1r, _2r, _1p, _2p, rc]


class Sat:

	def __init__(self, obs, site, path_to_file):
		self.obs = obs
		self.site = site
		self.path = path_to_file
		self.recparams = ""
		self.maxdelay = 900
		self.outparams = ""
		self.rawperiod = 3600
		self.outproducts = ""
		self.prodperiod = 3600
		self.lasttime = time.time()
		self.lat = -1
		self.lon = -1
		self.neighbors = ""
		self.stat = None
		self.file = None
		self.scheduler = BlockingScheduler({'apscheduler.job_defaults.max_instances': '1000', 'apscheduler.job_defaults.coalesce': 'True', 'apscheduler.job_defaults.misfire_grace_time':'600'});


	def watch_file(self):
		if len(list(glob.iglob(os.path.join(path, '*.jps')))) == 0:
			return False
		self.file = max(glob.iglob(os.path.join(self.path, '*.jps')), key=os.path.getctime)
		if not self.stat or self.stat.parser.fname != self.file:
			self.stat = JPSStatistics(self.file, mode='a')
		return True

	def update_measure(self):
		times = self.stat.parser.get_data('~~')
		if len(times) > 0 and os.path.exists(self.stat.parser.fname) == True:
			self.lasttime = time.time()
			return True

		if time.time() - self.lasttime <= self.maxdelay:
			return True

		return False

	def update_outrawparams(self):
		if self.outparams == "":
			return ""

		fields = self.outparams.split(',')
		ltime = self.stat.parser.get_data('~~')
		index = len(ltime) - 1
		out_str = ""
		for field in fields:
			field = field.strip()
			fdata = self.stat.parser.get_data(field)
			print("field: ", field)
			if fdata == None or len(fdata) <= index:
				out_str += 'None, '
			else:
				out_str += (str(fdata[index]) + ', ')
		return out_str[:-2]


	def update_outprodparams(self):
		if self.outproducts == "":
			return ""
