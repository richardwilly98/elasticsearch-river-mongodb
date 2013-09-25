// This script requires a change in elasticsearch-lang-javascript plugin tu spport CommonJS
var helper = require('uuidhelpers')
var base64 = '8Fq3Wd+BGUGD2CbsA4wasg==';
var subtype = 3;
var good = '59b75af0-81df-4119-83d8-26ec038c1ab2';
logger.debug("Convert subtype/base64 {} - {}", subtype, base64);
logger.debug("toUUID {}", helper.toUUID(base64));
logger.debug("toJUUID {}", helper.toJUUID(base64));
logger.debug("toCSUUID {}", helper.toCSUUID(base64));
logger.debug("toPYUUID {}", helper.toPYUUID(base64));
logger.debug("toHexUUID {}", helper.toHexUUID(subtype, base64));
logger.debug("Result should be {}", good);
