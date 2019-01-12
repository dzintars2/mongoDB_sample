#MongoDB
import os, sys, json, pymongo, random
from json import dumps
from flask import Flask, g, Response, request, render_template
from pathlib import Path
from dateutil import parser
import sampleData
from faker import Faker #nejaušu datu ģerenators


app = Flask(__name__)
app.debug = True

#DB MongoDB Atlas
client = pymongo.MongoClient("localhost", 27017)
db = client.test
mydb = client["lietvediba"]
tableDokVeidi = mydb["dokumentu_veidi"]
tableDarbinieki = mydb["darbinieki"]
tableUznemumi = mydb["uznemumi"]
tableDokumenti = mydb["dokumenti"]

fake = Faker('lv_LV') #neīstu datu ģenerators


@app.teardown_appcontext
def close_db(error):
    if hasattr(g, 'neo4j_db'):
        g.neo4j_db.close()

@app.route("/")
def get_index():
	text = '<img src="https://webassets.mongodb.com/_com_assets/cms/mongodb-logo-rgb-j6w271g1xn.jpg" width="100%">'
	return render_template('index.html', saturs=text)

#klasifikatoru ierakstu ģenerēšana
@app.route("/generateData")
def get_generateData():
	text = '<br>'
	tableDarbinieki.insert_many(sampleData.datiPersonas)
	tableDokVeidi.insert_many(sampleData.datiDokumentuVeidi)
	tableUznemumi.insert_many(sampleData.datiUznemumi)
	get_generateDocuments()
	text = '<br><table class="table table-hover table-sm"><thead><tr><th>Izveidota kolekcija</th><th>Ierakstu skaits</th></tr></thead><tbody>'
	#izdrukā kolekcijas un tajās esošo ierakstu skaitu
	for collection in mydb.list_collection_names():
		text += '<tr><td>' + collection + '</td><td>'+str(mydb[collection].count()) + '</td></tr>'
	text += '</tbody></table>'
	return render_template('index.html', saturs='<b>Klasifikatoru datu ģenerēšana</b>'+text)

#dokumentu datu ģenerēšana
@app.route("/generateDocuments")
def get_generateDocuments():
	text = '<br>'
	mongoData = []
	for i in range(500):
		sys.stdout.write("\rUzģenerēti %d ieraksti no 500" % i)
		sys.stdout.flush()
		isParent = 1
		while (isParent==1):
			#mūs interesē tikai dokumentu veidi, kuriem ir "parent_id" jeb kuri nav dokumentu veidu grupas
			dokumentaTips = tableDokVeidi.aggregate([{ "$sample": {"size": 1} }]).next()
			if ('parent_id' in dokumentaTips): isParent = 0
		nejaussDarbinieks = tableDarbinieki.aggregate([{ "$sample": {"size": 1} }]).next()
		datums = fake.date_between(start_date='-1y', end_date='now')
		datums = parser.parse(str(datums))
		if (dokumentaTips["_id"]=="rikojums_darb"):
			rikojumaNr = "2018-" + dokumentaTips["case_id"] + "/"+str(random.randint(1, 999))
			apraksts = fake.text()
			data = {"dokumentaTips": dokumentaTips["_id"], "persona": nejaussDarbinieks["_id"], 
			"numurs": rikojumaNr, "temats": "Rīkojums par darbu", "datums":datums, "apraksts": apraksts}
		elif (dokumentaTips["_id"]=="rikojums_visp"):
			rikojumaNr = "2018-" + dokumentaTips["case_id"] + "/"+str(random.randint(1, 999))
			data = {"dokumentaTips": dokumentaTips["_id"], "persona": nejaussDarbinieks["_id"], 
			"numurs": rikojumaNr, "temats": "Rīkojums", "datums": datums}
		elif (dokumentaTips["_id"]=="ligums_darba"):
			epastaAdrese = fake.email()
			amats = fake.job()
			bankasKonts = fake.iban()
			data = {"dokumentaTips": dokumentaTips["_id"], "ligumsledzejs": nejaussDarbinieks["_id"], 
			"epasts": epastaAdrese, "temats": "Darba līgums", "datums": datums, 
			"amats": amats, "bankas_konts":bankasKonts}
		elif (dokumentaTips["_id"]=="ligums_kredits" or dokumentaTips["_id"]=="ligums_saimn"):
			ligumsledzejs = tableUznemumi.aggregate([{ "$sample": {"size": 1} }]).next()
			if (dokumentaTips["_id"]=="ligums_kredits"):
				summa = (random.randint(1, 1000))*1000
				temats = "Aizdevuma līgums"
			else:
				summa = (random.randint(1, 9999999))/100
				temats = "Saimnieciskais līgums"
			apraksts = fake.text()
			apmaksasTermins = fake.date_between(start_date='+1y', end_date='+20y')
			apmaksasTermins = parser.parse(str(apmaksasTermins))
			ligumsledzejaPersona = fake.name()
			bankasKonts = fake.iban()
			pastaAdrese = fake.address()
			epastaAdrese = fake.email()
			amats = fake.job()
			data = {"dokumentaTips": dokumentaTips["_id"], "ligumsledzejs": ligumsledzejs["_id"], "pasta_adrese":pastaAdrese,
			"epasts": epastaAdrese, "darbinieks": nejaussDarbinieks["_id"],
			"summa": summa, "temats": temats, "datums": datums, "termins": apmaksasTermins, "apraksts": apraksts, 
			"ligumsledzejaPersona": ligumsledzejaPersona, "amats": amats, "bankas_konts":bankasKonts}
		mongoData.append(data)
	tableDokumenti.insert_many(mongoData)

	return render_template('index.html', saturs='<b>Dokumentu datu ģenerēšana</b>'+text)


@app.route("/deleteData")
def get_deleteData():
	tableDarbinieki.drop()
	tableDokVeidi.drop()
	tableUznemumi.drop()
	tableDokumenti.drop()
	return render_template('index.html', saturs='<b>Datu dzēšana</b><br>Kolekcijas izdzēstas')

#informācija par ierakstu skaitu kolekcijās
@app.route("/statistics")
def get_statistics():
	text = '<br><table class="table table-hover table-sm"><thead><tr><th>Kolekcija</th><th>Ierakstu skaits</th></tr></thead><tbody>'
	for collection in mydb.list_collection_names():
		text += '<tr><td>' + collection + '</td><td>'+str(mydb[collection].count()) + '</td></tr>'
	text += '</tbody></table><a class="nav-link" href="/generateDocuments">Papildus dokumentu ģenerēšana</a>'
	return render_template('index.html', saturs='<b>Statistika</b>'+text)

@app.route("/report1")
def get_report1():
	apraksts = """<b>1.atskaite</b><br>
			Kopsavilkums pa dokumentiem<br>
			(grupēts pa dokumementiem - summa (ja tāda ir noteikta), skaits (dati sakārtoti pēc dokumentu skaita)"""

	result = tableDokumenti.aggregate([
		{"$lookup": {
	         "from": "dokumentu_veidi",
	         "localField": "dokumentaTips",
	         "foreignField": "_id",
	         "as": "dokumenta_veidi_dati"
	       }
	    },
		{ "$group": {
			"_id": "$dokumentaTips", 
			"skaits": { "$sum": 1 }, 
			"summa": {"$sum": "$summa"}, 
			"dokumentaTipaNosaukums": {"$min":"$dokumenta_veidi_dati.name"}}
		},
		{ "$sort":{
			"skaits": -1}
		}])

	table = '<br><table class="table table-hover table-sm"><thead><tr><th>Dokumenta tips</th><th>Dokumentu skaits</th><th>Dokumentos norādīta kopsumma (ja norādīts)</th></tr></thead><tbody>'
	for ieraksts in result:
		table += '<tr><td>'+ieraksts["dokumentaTipaNosaukums"][0]+'</td><td>'+str(ieraksts["skaits"])+'</td><td>'+str('{:5.2f}'.format(ieraksts["summa"]))+'</td></tr>'
	table += '</tbody></table>'
	return render_template('index.html', saturs=apraksts+table)

@app.route("/report2")
def get_report2():
	apraksts = """<b>2.atskaite</b><br>
			Aizdevuma līgumu kopsavilkums<br>
			Atmaksas termiņa gads, atmaksājamā summa. Iekļauti aizdevumi ar atmaksas termiņu līdz 31.12.2029"""
	table = '<br><table class="table table-hover table-sm"><thead><tr><th>Termiņs (gads)</th><th>Atmaksājamā summa</th></tr></thead><tbody>'
	datums = parser.parse(str("2030-01-01"))
	result = tableDokumenti.aggregate([
		{"$match": {"dokumentaTips":"ligums_kredits", "termins": { "$lte" : datums}}},
		{ "$group": {
			"_id": { "gads": { "$year": "$termins" } }, 
			"summa": {"$sum": "$summa"}}},
			{"$sort":{"_id": 1}}	
		])
	for ieraksts in result:
		table += '<tr><td>'+str(ieraksts["_id"]["gads"])+'.gads</td><td>'+str(ieraksts["summa"])+'</td></tr>'
	table += '</tbody></table>'
	return render_template('index.html', saturs=apraksts+table)

@app.route("/report3")
def get_report3():
	apraksts = """<b>3.atskaite</b><br>
			Kopsavilkums pa rīkojumiem (darbinieks, rīkojumu skaits, TOP10 darbinieki pēc rīkojumu skaita)"""
	table = '<br><table class="table table-hover table-sm"><thead><tr><th>#</th><th>Darbinieks</th><th>Rīkojumu skaits</th></tr></thead><tbody>'
	result = tableDokumenti.aggregate([
		{"$lookup": {
	         "from": "darbinieki",
	         "localField": "persona",
	         "foreignField": "_id",
	         "as": "darbinieks"
	       }
	    },
	    {"$lookup": {
	         "from": "dokumentu_veidi",
	         "localField": "dokumentaTips",
	         "foreignField": "_id",
	         "as": "dokumenta_veidi_dati"
	       }
	    }, 
	    {"$addFields": {
	    	"dok_veida_grupa": "$dokumenta_veidi_dati.parent_id",
	    	"darbinieks_vards": "$darbinieks.name"
	    	}},
	    {"$match": {"dok_veida_grupa": "rikojums"}},
	    {"$group":{
	    	"_id": "$persona",
	    	"vards_uzvards": {"$min":"$darbinieks_vards"},
	    	"dokumentu_skaits": {"$sum": 1}
	    	}
	    }, 
	    {"$sort":{"dokumentu_skaits": -1}},
	    {"$limit": 10}
		])
	i = 1
	for ieraksts in result:
		table += '<tr><td>'+str(i)+'</td><td>' + ieraksts["vards_uzvards"][0] + '</td><td>' + str(ieraksts["dokumentu_skaits"]) + '</td></tr>'
		i += 1
	table += '</tbody></table>'
	return render_template('index.html', saturs=apraksts+table)

@app.route("/report4")
def get_report4():
	apraksts = """<b>4.atskaite</b><br>
			Līgumi pa līgumslēdzējiem<br>
			Darījuma partnera nosaukums, līguma veids, līgumu slēgšanas periods (līguma datums min-max), 
			līgumu skaits, TOP10 partneri pēc līgumu skaita
			"""
	table = '<br><table class="table table-hover table-sm"><thead><tr><th>#</th><th>Partneris</th><th>Līguma veids</th><th>Līgumu periods</th><th>Līgumu skaits</th><th>Līgumu kopsumma</th></tr></thead><tbody>'
	result = tableDokumenti.aggregate([
		{"$lookup": {
	         "from": "uznemumi",
	         "localField": "ligumsledzejs",
	         "foreignField": "_id",
	         "as": "uznemums"
	       }
	    },
	    {"$lookup": {
	         "from": "dokumentu_veidi",
	         "localField": "dokumentaTips",
	         "foreignField": "_id",
	         "as": "dokumenta_veidi_dati"
	       }
	    }, 
	    {"$addFields": {
	    	"dok_veida_grupa": "$dokumenta_veidi_dati.parent_id",
	    	"dok_veida_nosaukums": "$dokumenta_veidi_dati.name",
	    	"partneris_nos": "$uznemums.name_in_quotes", 
	    	"partnera_tips": "$uznemums.type"
	    	}},
	    {"$match": 
	    	{"dokumentaTips": {"$in": ["ligums_kredits","ligums_saimn"]}}
	    },
	    {"$group":{
	    	"_id": {"partneraId":"$ligumsledzejs",
	    			"dok_veids": "$dokumentaTips"},
	    	"dok_veida_nosaukums": {"$min": "$dok_veida_nosaukums"},
	    	"partneris": {"$min":"$partneris_nos"},
	    	"partn_tips": {"$min":"$partnera_tips"},
	    	"ligumsumma": {"$sum":"$summa"},
	    	"datums_no": {"$min":"$datums"},
	    	"datums_lidz": {"$max": "$datums"},
	    	"dokumentu_skaits": {"$sum": 1}
	    	}
	    }, 
	    {"$sort":{"dokumentu_skaits": -1}},
	    {"$limit": 10}
		])
	i = 1
	for ieraksts in result:
		table += ('<tr><td>'+str(i)+'</td><td>'+ieraksts["partneris"][0]+' '+ieraksts["partn_tips"][0]+'</td><td>'+
			ieraksts["dok_veida_nosaukums"][0] + '</td><td>'+ str(ieraksts["datums_no"])[0:10] + ' - ' + str(ieraksts["datums_lidz"])[0:10] +
			'</td><td>'+str(ieraksts["dokumentu_skaits"]) + '</td><td>'+str('{:5.2f}'.format(ieraksts["ligumsumma"])) + '</td></tr>')
		i += 1
	table += '</tbody></table>'
	return render_template('index.html', saturs=apraksts+table)

@app.route("/report5")
def get_report5():
	apraksts = """<b>5.atskaite</b><br>
			Lielākie līgumi pa darbiniekiem, kuri tos ir slēguši.
			Sakārtots pēc lielākās līgumsummas. TOP10 darbinieki.
			"""
	table = '<br><table class="table table-hover table-sm"><thead><tr><th>#</th><th>Darbinieks</th><th>Līgumu skaits</th><th>Līgumsummu kopsumma</th><th>Maksimālā līgumsumma</th></tr></thead><tbody>'
	result = tableDokumenti.aggregate([
		{"$lookup": {
	         "from": "darbinieki",
	         "localField": "darbinieks",
	         "foreignField": "_id",
	         "as": "darbinieka_dati"
	       }
	    },
	    {"$lookup": {
	         "from": "dokumentu_veidi",
	         "localField": "dokumentaTips",
	         "foreignField": "_id",
	         "as": "dokumenta_veidi_dati"
	       }
	    }, 
	    {"$addFields": {
	    	"dok_veida_grupa": "$dokumenta_veidi_dati.parent_id",
	    	"darbinieka_vards": "$darbinieka_dati.name"
	    	}},
	    {"$match": 
	    	{"dokumentaTips": {"$in": ["ligums_kredits","ligums_saimn"]}}
	    },
	    {"$group":{
	    	"_id": {"partneraId":"$darbinieks"},
	    	"darbinieks": {"$min":"$darbinieka_vards"},
	    	"ligumu_kopsumma": {"$sum":"$summa"},
	    	"max_ligums": {"$max":"$summa"},
	    	"dokumentu_skaits": {"$sum": 1}
	    	}
	    }, 
	    {"$sort":{"max_ligums": -1}},
	    {"$limit": 10}
		])
	i = 1
	for ieraksts in result:
		table += ('<tr><td>'+str(i)+'</td><td>'+ieraksts["darbinieks"][0] + '</td><td>' +
				str(ieraksts["dokumentu_skaits"]) + '</td><td>' + str('{:5.2f}'.format(ieraksts["ligumu_kopsumma"])) + 
				'</td><td>' + str('{:5.2f}'.format(ieraksts["max_ligums"])) + '</td></tr>')
		i += 1
	table += '</tbody></table>'
	return render_template('index.html', saturs=apraksts+table)

if __name__ == '__main__':
    app.run(port=8080)

