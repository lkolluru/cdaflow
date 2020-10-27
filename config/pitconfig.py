

class PitConfig(_CdaConfigParser):
	def _validate(self):
		pass
	
	@property
	def pit_obj_list(self):
		_pit_obj_list = []
		for key, value in enumerate(self._flow_pit_config['bigdatasnapshots']):
			_pit_obj_list.append(value)
		return _pit_obj_list
	
	@property
	def pit_snapshot_list(self):
		_pit_obj_list = []
		for key, value in enumerate(self._flow_pit_config['bigdatasnapshots']):
			_pit_obj_list.append(value)
		return _pit_obj_list


"""
	Point in Time Configuration Objects
"""

PITDETAILS = pd.DataFrame(columns=['bd_tablename', 'bd_snapshotdate',
                                   'bd_snapshotlocation', 'cda_tablename',
                                   'flow_script_location', 'bd_snapshotid'])

PITPLACEHOLDER = pd.DataFrame(columns=['bd_tablename', 'bd_snapshotid',
                                       'bd_snapshotlocation', 'bd_datalocation',
                                       'bd_volume', 'cda_tablename', 'flow_script_location'])

PITADDPARTITIONSCRIPT = "/gen_sql/omnicorenested/spartitions/add_snapshot_partitions.sql"

"""
Customer Snapshot Objects
"""


class Tdmcust(PitConfig):
	@property
	def bd_tablename(self):
		return self._flow_pit_config['bigdatasnapshots']['tdmcust']['bigdatatablename']
	
	@property
	def bd_snapshotlocation(self):
		return self._flow_pit_config['bigdatasnapshots']['tdmcust']['snapshotlocation']
	
	@property
	def bd_datalocation(self):
		return self._flow_pit_config['bigdatasnapshots']['tdmcust']['datalocation']
	
	@property
	def bd_volume(self):
		return self._flow_pit_config['bigdatasnapshots']['tdmcust']['bigdatavolumename']
	
	@property
	def cda_tablename(self):
		return self._flow_pit_config['bigdatasnapshots']['tdmcust']['cdatablename']
	
	@property
	def flow_script_location(self):
		return self._flow_pit_config['bigdatasnapshots']['tdmcust']['ddlscriptlocation']


class Tdmordq(PitConfig):
	@property
	def bd_tablename(self):
		return self._flow_pit_config['bigdatasnapshots']['tdmordq']['bigdatatablename']
	
	@property
	def bd_snapshotlocation(self):
		return self._flow_pit_config['bigdatasnapshots']['tdmordq']['snapshotlocation']
	
	@property
	def bd_datalocation(self):
		return self._flow_pit_config['bigdatasnapshots']['tdmordq']['datalocation']
	
	@property
	def bd_volume(self):
		return self._flow_pit_config['bigdatasnapshots']['tdmordq']['bigdatavolumename']
	
	@property
	def cda_tablename(self):
		return self._flow_pit_config['bigdatasnapshots']['tdmordq']['cdatablename']
	
	@property
	def flow_script_location(self):
		return self._flow_pit_config['bigdatasnapshots']['tdmordq']['ddlscriptlocation']


class Tdmordr(PitConfig):
	@property
	def bd_tablename(self):
		return self._flow_pit_config['bigdatasnapshots']['tdmordr']['bigdatatablename']
	
	@property
	def bd_snapshotlocation(self):
		return self._flow_pit_config['bigdatasnapshots']['tdmordr']['snapshotlocation']
	
	@property
	def bd_datalocation(self):
		return self._flow_pit_config['bigdatasnapshots']['tdmordr']['datalocation']
	
	@property
	def bd_volume(self):
		return self._flow_pit_config['bigdatasnapshots']['tdmordr']['bigdatavolumename']
	
	@property
	def cda_tablename(self):
		return self._flow_pit_config['bigdatasnapshots']['tdmordr']['cdatablename']
	
	@property
	def flow_script_location(self):
		return self._flow_pit_config['bigdatasnapshots']['tdmordr']['ddlscriptlocation']


class Tdmorit(PitConfig):
	@property
	def bd_tablename(self):
		return self._flow_pit_config['bigdatasnapshots']['tdmorit']['bigdatatablename']
	
	@property
	def bd_snapshotlocation(self):
		return self._flow_pit_config['bigdatasnapshots']['tdmorit']['snapshotlocation']
	
	@property
	def bd_datalocation(self):
		return self._flow_pit_config['bigdatasnapshots']['tdmorit']['datalocation']
	
	@property
	def bd_volume(self):
		return self._flow_pit_config['bigdatasnapshots']['tdmorit']['bigdatavolumename']
	
	@property
	def cda_tablename(self):
		return self._flow_pit_config['bigdatasnapshots']['tdmorit']['cdatablename']
	
	@property
	def flow_script_location(self):
		return self._flow_pit_config['bigdatasnapshots']['tdmorit']['ddlscriptlocation']


class Tdmcusu(PitConfig):
	@property
	def bd_tablename(self):
		return self._flow_pit_config['bigdatasnapshots']['tdmcusu']['bigdatatablename']
	
	@property
	def bd_snapshotlocation(self):
		return self._flow_pit_config['bigdatasnapshots']['tdmcusu']['snapshotlocation']
	
	@property
	def bd_datalocation(self):
		return self._flow_pit_config['bigdatasnapshots']['tdmcusu']['datalocation']
	
	@property
	def bd_volume(self):
		return self._flow_pit_config['bigdatasnapshots']['tdmcusu']['bigdatavolumename']
	
	@property
	def cda_tablename(self):
		return self._flow_pit_config['bigdatasnapshots']['tdmcusu']['cdatablename']
	
	@property
	def flow_script_location(self):
		return self._flow_pit_config['bigdatasnapshots']['tdmcusu']['ddlscriptlocation']


class Tdmefcu(PitConfig):
	@property
	def bd_tablename(self):
		return self._flow_pit_config['bigdatasnapshots']['tdmefcu']['bigdatatablename']
	
	@property
	def bd_snapshotlocation(self):
		return self._flow_pit_config['bigdatasnapshots']['tdmefcu']['snapshotlocation']
	
	@property
	def bd_datalocation(self):
		return self._flow_pit_config['bigdatasnapshots']['tdmefcu']['datalocation']
	
	@property
	def bd_volume(self):
		return self._flow_pit_config['bigdatasnapshots']['tdmefcu']['bigdatavolumename']
	
	@property
	def cda_tablename(self):
		return self._flow_pit_config['bigdatasnapshots']['tdmefcu']['cdatablename']
	
	@property
	def flow_script_location(self):
		return self._flow_pit_config['bigdatasnapshots']['tdmefcu']['ddlscriptlocation']


class Tdmnono(PitConfig):
	@property
	def bd_tablename(self):
		return self._flow_pit_config['bigdatasnapshots']['tdmnono']['bigdatatablename']
	
	@property
	def bd_snapshotlocation(self):
		return self._flow_pit_config['bigdatasnapshots']['tdmnono']['snapshotlocation']
	
	@property
	def bd_datalocation(self):
		return self._flow_pit_config['bigdatasnapshots']['tdmnono']['datalocation']
	
	@property
	def bd_volume(self):
		return self._flow_pit_config['bigdatasnapshots']['tdmnono']['bigdatavolumename']
	
	@property
	def cda_tablename(self):
		return self._flow_pit_config['bigdatasnapshots']['tdmnono']['cdatablename']
	
	@property
	def flow_script_location(self):
		return self._flow_pit_config['bigdatasnapshots']['tdmnono']['ddlscriptlocation']


class Tdmorak(PitConfig):
	@property
	def bd_tablename(self):
		return self._flow_pit_config['bigdatasnapshots']['tdmcudq']['bigdatatablename']
	
	@property
	def bd_snapshotlocation(self):
		return self._flow_pit_config['bigdatasnapshots']['tdmorak']['snapshotlocation']
	
	@property
	def bd_datalocation(self):
		return self._flow_pit_config['bigdatasnapshots']['tdmorak']['datalocation']
	
	@property
	def bd_volume(self):
		return self._flow_pit_config['bigdatasnapshots']['tdmorak']['bigdatavolumename']
	
	@property
	def cda_tablename(self):
		return self._flow_pit_config['bigdatasnapshots']['tdmorak']['cdatablename']
	
	@property
	def flow_script_location(self):
		return self._flow_pit_config['bigdatasnapshots']['tdmorak']['ddlscriptlocation']


class Tdmcudq(PitConfig):
	@property
	def bd_tablename(self):
		return self._flow_pit_config['bigdatasnapshots']['tdmcudq']['bigdatatablename']
	
	@property
	def bd_snapshotlocation(self):
		return self._flow_pit_config['bigdatasnapshots']['tdmcudq']['snapshotlocation']
	
	@property
	def bd_datalocation(self):
		return self._flow_pit_config['bigdatasnapshots']['tdmcudq']['datalocation']
	
	@property
	def bd_volume(self):
		return self._flow_pit_config['bigdatasnapshots']['tdmcudq']['bigdatavolumename']
	
	@property
	def cda_tablename(self):
		return self._flow_pit_config['bigdatasnapshots']['tdmcudq']['cdatablename']
	
	@property
	def flow_script_location(self):
		return self._flow_pit_config['bigdatasnapshots']['tdmcudq']['ddlscriptlocation']


class Tdmtran(PitConfig):
	@property
	def bd_tablename(self):
		return self._flow_pit_config['bigdatasnapshots']['tdmtran']['bigdatatablename']
	
	@property
	def bd_snapshotlocation(self):
		return self._flow_pit_config['bigdatasnapshots']['tdmtran']['snapshotlocation']
	
	@property
	def bd_datalocation(self):
		return self._flow_pit_config['bigdatasnapshots']['tdmtran']['datalocation']
	
	@property
	def bd_volume(self):
		return self._flow_pit_config['bigdatasnapshots']['tdmtran']['bigdatavolumename']
	
	@property
	def cda_tablename(self):
		return self._flow_pit_config['bigdatasnapshots']['tdmtran']['cdatablename']
	
	@property
	def flow_script_location(self):
		return self._flow_pit_config['bigdatasnapshots']['tdmtran']['ddlscriptlocation']


"""
Prospect Snapshot Objects
"""


class Tnbefhh(PitConfig):
	@property
	def bd_tablename(self):
		return self._flow_pit_config['bigdatasnapshots']['tnbefhh']['bigdatatablename']
	
	@property
	def bd_snapshotlocation(self):
		return self._flow_pit_config['bigdatasnapshots']['tnbefhh']['snapshotlocation']
	
	@property
	def bd_datalocation(self):
		return self._flow_pit_config['bigdatasnapshots']['tnbefhh']['datalocation']
	
	@property
	def bd_volume(self):
		return self._flow_pit_config['bigdatasnapshots']['tnbefhh']['bigdatavolumename']
	
	@property
	def cda_tablename(self):
		return self._flow_pit_config['bigdatasnapshots']['tnbefhh']['cdatablename']
	
	@property
	def flow_script_location(self):
		return self._flow_pit_config['bigdatasnapshots']['tnbefhh']['ddlscriptlocation']


class Tnbxref(PitConfig):
	@property
	def bd_tablename(self):
		return self._flow_pit_config['bigdatasnapshots']['tnbxref']['bigdatatablename']
	
	@property
	def bd_snapshotlocation(self):
		return self._flow_pit_config['bigdatasnapshots']['tnbxref']['snapshotlocation']
	
	@property
	def bd_datalocation(self):
		return self._flow_pit_config['bigdatasnapshots']['tnbxref']['datalocation']
	
	@property
	def bd_volume(self):
		return self._flow_pit_config['bigdatasnapshots']['tnbxref']['bigdatavolumename']
	
	@property
	def cda_tablename(self):
		return self._flow_pit_config['bigdatasnapshots']['tnbxref']['cdatablename']
	
	@property
	def flow_script_location(self):
		return self._flow_pit_config['bigdatasnapshots']['tnbxref']['ddlscriptlocation']

