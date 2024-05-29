const mod = require('.');
const fs = require('fs');

if (process.argv.length < 4) {
    console.error(`Usage: node ${process.argv[1]} <scenario name in DB> <results CSV prefix>`);
} else {
    const model = mod.runModel('db/generator/test.db', process.argv[2]);
    mod.exportModel(model, process.argv[3]);
    fs.writeFileSync(
        `${process.argv[3]}-metrics.json`,
        JSON.stringify(mod.readModel(model).metrics), {encoding: 'utf-8'});
}
