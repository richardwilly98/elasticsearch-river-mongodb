logger.debug("Incoming document: {}", ctx.document)
def now = new Date()
println "Now: ${now}"
ctx.document.modified = now.clearTime()
logger.debug("Outgoing document: {}", ctx.document)