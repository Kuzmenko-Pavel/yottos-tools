// Скрипт проверяет состояние локальной базы данных getmyad (которая используется worker'ами)
// При необходимости восстанавливает
if (!db.log.click.validate().valid || !db.log.click.stats().capped) {
	print ("Database log.impressions broken, recreating collections");	
	db.log.click.drop();
	db.createCollection('log.click', {capped: true, max: 5000000, size: 2500000000});
}
if (!db.log.impressions.validate().valid || !db.log.impressions.stats().capped) {
	print ("Database log.impressions.block broken, recreating collections");	
	db.log.impressions.drop();
	db.createCollection('log.impressions', {capped: true, max: 5000000, size: 2500000000});
}
