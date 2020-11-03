import paho.mqtt.client as mqtt
from sat import Sat


def on_connect(client, userdata, flags, rc):
	if rc == 0:
		print("Connected to broker")
		global Connected
		Connected = True
	else:
		print("Connection failed")

def on_message(client, userdata, message):
	print(message.topic, message.payload)
	if "recparams" in message.topic:
		cur_sat.recparams = message.payload.decode()
	if "maxdelay" in message.topic:
		cur_sat.maxdelay = int(message.payload.decode())
	if "outparams" in message.topic:
		cur_sat.outparams = message.payload.decode()
	if "rawperiod" in message.topic:
		cur_sat.rawperiod = int(str(message.payload.decode()))
		scheduler.reschedule_job('outrawparams',trigger='interval',seconds=RAWPERIOD)
	if "outproducts" in message.topic:
		cur_sat.outproducts = message.payload.decode()
	if "prodperiod" in message.topic:
		global PRODPERIOD
		PRODPERIOD = int(message.payload.decode())
		scheduler.reschedule_job('outproducts',trigger='interval',seconds=PRODPERIOD)
	if "lat" in message.topic:
		cur_sat.lat = float(message.payload.decode())
	if "lon" in message.topic:
		cur_sat.lon = float(message.payload.decode())
	if "neighbors" in message.topic:
		cur_sat.neighbors = message.payload.decode()

def connection_to_broker():
	broker_address = "62.109.10.163"
	port = 1883
	user = "station"
	password = ""

	client.on_connect = on_connect
	client.on_message = on_message
	client.connect(broker_address, port=port)

	client.loop_start()

	while Connected != True:
		time.sleep(0.1)

def on_sub():
	client.subscribe("recparams/" + cur_sat.obs + '/' + cur_sat.site)
	client.subscribe("maxdelay/" + cur_sat.obs + '/' + cur_sat.site)
	client.subscribe("outparams/" + cur_sat.obs + '/' + cur_sat.site)
	client.subscribe("rawperiod/" + cur_sat.obs + '/' + cur_sat.site)
	client.subscribe("outproducts/" + cur_sat.obs + '/' + cur_sat.site)
	client.subscribe("prodperiod/" + cur_sat.obs + '/' + cur_sat.site)
	client.subscribe("lat/" + cur_sat.obs + '/' + cur_sat.site)
	client.subscribe("lon/" + cur_sat.obs + '/' + cur_sat.site)
	client.subscribe("neighbors/" + cur_sat.obs + '/' + cur_sat.site)

def watch_file():
	if cur_sat.watch_file() == False:
		cur_sat.scheduler.pause()
		client.publish("errorFile/" + OBS + '/' + SITE, "1", retain=True)
		while cur_sat.watch_file() == False:
			if time.localtime().tm_min == 0 and time.localtime.tm_sec == 0:
				client.publish("availability" + OBS + '/' + SITE, "0", retain=True)
			print("wait file")
		client.publish("errorFile/" + OBS + '/' + SITE, "0", retain=True)
		cur_sat.scheduler.resume()

def start_settings():
	client.publish("availability/" + OBS + '/' + SITE, "-1", retain=True)
	client.publish("measure/" + OBS + '/' + SITE, "0", retain=True)
	client.publish("lasttime/" + OBS + '/' + SITE, "0", retain=True)
	client.publish("maxdelay/" + OBS + '/' + SITE, MAXDELAY, retain=True)
	update_measure()
	update_lasttime()


def update_measure():
	if cur_sat.update_measure() == True:
		client.publish("measure/" + OBS + '/' + SITE, "1", retain=True)
	else:
		client.publish("measure/" + OBS + '/' + SITE, "0", retain=True)



def update_lasttime():
	client.publish("lasttime/" + OBS + '/' + SITE, str(LASTTIME), retain=True)


def update_realparams():
	params = ",".join(stat.parser.get_codes_in_file())
	client.publish("realrecparams/" + OBS + '/' + SITE, params, retain=True)


def update_outrawparams():
	data = cur_sat.update_outrawparams()
	client.publish("outrawdata/" + OBS + '/' + SITE, data,  retain=True)


def update_outprodparams():
	data = cur_sat.update_outprodparams()
	client.publish("outproddata/" + OBS + '/' + SITE, "", retain=True)


def update_stat():
	print('Updating statistics')
	watch_file()
	cur_sat.stat.update()
	cur_sat.stat.get_data_availability(accumulate=True)


def update_availability():
	proc = cur_sat.stat.get_data_availability(accumulate=False)
	client.publish("availability/" + OBS + '/' + SITE,str(proc*100),retain=True)


def data_proc(sat):
	sat.scheduler.add_job(func=update_stat,trigger='interval',seconds=600)
	sat.scheduler.add_job(func=update_measure,trigger='interval',seconds=600)
	sat.scheduler.add_job(func=update_realparams,trigger='interval',seconds=3600)
	sat.scheduler.add_job(func=update_outrawparams,trigger='interval',seconds=RAWPERIOD, id='outrawparams')
	sat.scheduler.add_job(func=update_outprodparams,trigger='interval',seconds=PRODPERIOD, id='outproducts')
	sat.scheduler.add_job(func=update_availability,trigger='cron',hour='*')
	sat.scheduler.add_job(func=update_lasttime,trigger='interval',seconds=600)

#----------------------------------------#
Connected = False
client = mqtt.Client(OBS + '/' + SITE)

connection_to_broker()
on_sub()

#-----------------------------------------#

cur_sat = new Sat("obs1", "site1", "/home/limbo4/.limbo_data/")

watch_file()
start_settings()
data_proc(cur_sat)
try:
	cur_sat.scheduler.start()
	print('SCHEDULER', cur_stat.scheduler.state())
except (KeyboardInterrupt, SystemExit):
	client.publish("exit/" + OBS + '/' + SITE, "1", retain=False)
	scheduler.shutdown()
	client.disconnect()
	client.loop_stop()
