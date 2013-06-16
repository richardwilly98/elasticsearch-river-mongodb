ctx.document.modified = ctx.document.created;
ctx.document.created = ctx.document.created.getTime();
ctx.document.flag = true;