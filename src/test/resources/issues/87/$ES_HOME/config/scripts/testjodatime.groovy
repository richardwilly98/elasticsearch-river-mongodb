import org.joda.time.DateTime
logger.debug("Incoming document: {}", ctx.document)
def today = new DateTime()
println "Today: ${today}"
ctx.document.monthOfYear = today.monthOfYear
logger.debug("Outgoing document: {}", ctx.document)