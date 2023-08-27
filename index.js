const fs = require("fs");
const archiver = require("archiver");
const dayjs = require('dayjs');
// 加载日志模块
// const log4js = require("log4js");
// log4js.configure({
//     appenders: {
//         default: {
//             type: "console",
//             filename: "./logs/process.log",
//         }, cheese: { type: "file", filename: `./logs/process.log` }
//     },
//     categories: { default: { appenders: ["default","cheese"], level: "debug" } },
// });

const logger = {
    error: (params) => {
        console.error(new Date().toLocaleString() + "[error]" + params)
    },
    debug: (params) => {
        console.log(new Date().toLocaleString() + "[debug]" + params)
    },
    info: (params) => {
        console.log(new Date().toLocaleString() + "[info]" + params)
    }
}

// 加载config文件

let configFile = process.argv[2];
if(!configFile){
    return  logger.error("需要指定config文件名")
}
const config = require(`./${configFile}`);
let {
    startTime,
    endTime,
    dbInfo,
    intervalData,
    intervalTable,
    createTableFlag
} = config;
if (startTime.length !== 10) {
    return logger.error("开始时间请输入正确日期");
} else {
    startTime = startTime + " 00:00:00.0"
    endTime = endTime + " 00:00:00.0"
}
if (createTableFlag === undefined) {
    createTableFlag = true;
}
const dataLocation = "khlData";
const syncStartTime = startTime ? new Date(startTime).getTime() : new Date().getTime();
let stopFlagTime = syncStartTime;
let stopFlag = false;
const frequencyTime = 24 * 60 * 60 * 1000
const syncEndTime = endTime ? new Date(endTime).getTime() : 0;
let intervalList_min = intervalTable;
let _cbList = []; // 所要同步的点列表

let childTableMap = new Map();
let tagTypeMap = new Map();
let cacheTime = new Map();
let timer = null;
let dayFlag = 0;
// 实例化TaosCLI连接Tdengine类
const masterDBCliClass = require("./src/dbexec_cli").getInstance(
    dbInfo.host,
    dbInfo.auth
);
const { KDBTDEngine } = require("./src/dbquery_rest");
const masterDBClass = new KDBTDEngine(dbInfo);
// 检查存储csv的文件夹是否存在，文件夹名字为进程序号
function checkTempFloder() {
    dbUrl = `./${dataLocation}`;
    // 判断是否存在文件夹
    if (!fs.existsSync(dbUrl)) {
        fs.mkdirSync(dbUrl);
        fs.mkdirSync(dbUrl + "/data")
    } else {
        // logger.error(dbUrl + "已存在，进程退出,请删除文件夹")
        // process.exit()
        logger.info(dbUrl + "已存在,数据会覆盖此文件夹")
    }
}
// 连接数据库
async function connectDB() {
    try {
        let ret = await masterDBClass.connect();
        logger.info("Master 连接结果:" + ret);
    } catch (error) {
        return logger.error(error);
    }
}

async function getChildTable() {
    // where tagname Like "%R安化北里%"
    let queryTBSql = `select distinct tbname,tagctb,tagname,tagtype from history_taglist`;
    let ret = await masterDBClass.query(queryTBSql);
    if (ret.code) {
        logger.error(`history_taglist表查询失败`);
        return -1;
    } else {
        logger.debug("history_taglist表查询成功")
        for (let i = 0; i < ret.data.length; i++) {
            const element = ret.data[i];
            _cbList.push(element[1]);
            cacheTime.set(element[1], syncStartTime);
            childTableMap.set(element[1], element[2]);
            tagTypeMap.set(element[1], element[3]);

        }
    }

}
async function createTable() {
    logger.info(`开始备份表`);
      try {
        if(fs.statSync(`./${dataLocation}/sqlList/`).isDirectory() == false){
            fs.mkdirSync(`./${dataLocation}/sqlList/`)
        }
    } catch (error) {
        fs.mkdirSync(`./${dataLocation}/sqlList/`)
    }
    let sqlList = [];
    let taglistSql = `CREATE STABLE IF NOT EXISTS history_taglist (tm TIMESTAMP, uv INT) TAGS (tagname NCHAR(256), tagtype INT, tagctb NCHAR(256))`;
    sqlList.push(taglistSql);
    for (let j = 0; j < intervalList_min.length; j++) {
        const element = intervalList_min[j];
        let minSql = `CREATE STABLE IF NOT EXISTS history_${element}min (datatime TIMESTAMP, dataquality INT,datavalue NCHAR(128)) TAGS (tagname NCHAR(128))`;
        sqlList.push(minSql);
    }
    for (let i = 0; i < _cbList.length; i++) {
        const tagCb = _cbList[i];
        let tag = childTableMap.get(tagCb);
        let tagType = tagTypeMap.get(tagCb)
        let tagType_str = ""
        switch (tagType) {
            case 1:
                tagType_str = "bool";
                break;
            case 4:
                tagType_str = "int";
                break;
            case 6:
                tagType_str = "float"
                break;
            case 7:
                tagType_str = "double"
                break;
            default:
                continue
        }
        let createhisTable = `CREATE TABLE IF NOT EXISTS hist_tl_${tagCb} USING history_taglist (tagname ,tagtype, tagctb ) TAGS ("${tag}",${tagType},"${tagCb}")`;
        let createTable = `CREATE TABLE IF NOT EXISTS ${tagCb} USING history_${tagType_str} (tagname ,description, id, unit,groupid) TAGS ("${tag}", "", 0, null, 0 )`;
        sqlList.push(createhisTable);
        sqlList.push(createTable);
        for (let j = 0; j < intervalList_min.length; j++) {
            const element = intervalList_min[j];
            let minSql = `CREATE TABLE IF NOT EXISTS ${tagCb}_${element}min USING history_${element}min TAGS ("${tag}")`;
            sqlList.push(minSql);
        }
        if (i % 10 == 0) {
            logger.debug(`备份表进度： ${Math.floor(i * 100 / _cbList.length)}%`);
        }
    }
    let groupSqlList = group(sqlList,50000)
    for (let j = 0; j < groupSqlList.length; j++) {
        fs.writeFileSync(`./${dataLocation}/sqlList/sqlList${j+1}.sql`, "");
        const sqlList = groupSqlList[j];
        for (let i = 0; i < sqlList.length; i++) {
            const element = sqlList[i];
            await writeDataToFile(`./${dataLocation}/sqlList/sqlList${j+1}.sql`, element + "\n");
            if (i % 10 == 0) {
                logger.debug(`总计${groupSqlList.length}张表,写第${j+1}表文件进度： ${Math.floor(i * 100 / sqlList.length)}%`);
            }
        }
    }
    
    logger.info(`备份表完毕`)

}

async function writeDataToFile(filePath, data) {
    let writerStream = fs.createWriteStream(filePath, { flags: 'a' });
    writerStream.write(data, 'UTF8');
    writerStream.end();
    await new Promise((resolve, reject) => {
        writerStream.on('finish', resolve);
        writerStream.on('error', reject);
    });
}


// 获取变量的最后一条记录更新时间
async function saveTagSaveTime() {
    let obj = {};
    cacheTime.forEach((value, key) => {
        obj[key] = {
            0: childTableMap.get(key),
            1: startTime,
            2: ts_to_time(value)
        }
    });

    fs.writeFileSync(`./${dataLocation}/tagInfo.json`, "");
    await writeDataToFile(`./${dataLocation}/tagInfo.json`, JSON.stringify(obj));
}
// 时间戳转字符时间 YYYY-MM-DD HH:MM:SS
function ts_to_time(n) {
    // let now = new Date(n),
    //     y = now.getFullYear(),
    //     m = now.getMonth() + 1,
    //     d = now.getDate();
    // return (
    //     y +
    //     "-" +
    //     (m < 10 ? "0" + m : m) +
    //     "-" +
    //     (d < 10 ? "0" + d : d) +
    //     " " +
    //     now.toTimeString().substr(0, 8)
    // );
    return dayjs(new Date(n)).format("YYYY-MM-DD HH:mm:ss")
}
async function writeGroupInfo(cbGroup) {
    let obj = {};
    for (let id = 0; id < cbGroup.length; id++) {
        const element = cbGroup[id];
        let tagArr = [];
        for (let j = 0; j < element.length; j++) {
            const cb = element[j];
            tagArr.push(childTableMap.get(cb))
        }
        obj[`group${id}`] = tagArr;
    }
    fs.writeFileSync(`./${dataLocation}/tag_group.json`, JSON.stringify(obj));
}

// 变量分组函数
function group(array, subGroupLength) {
    let index = 0;
    let newArray = [];
    while (index < array.length) {
        newArray.push(array.slice(index, (index += subGroupLength)));
    }
    return newArray;
}
async function backup(cbGroup) {
    logger.debug("开始备份" + ts_to_time(stopFlagTime) + "数据")
    await saveTagSaveTime()
    //判断是否要执行停止同步
    if (syncEndTime && stopFlagTime > syncEndTime) {
        stopFlag = true;
    }
    let yearStr = new Date(stopFlagTime).getFullYear();
    let monThStr = new Date(stopFlagTime).getMonth() + 1;
    let dayStr = new Date(stopFlagTime).getDate();
    // 压缩前一天文件为tar.gz
    if (dayFlag !== dayStr && dayFlag !== 0) {
        await mkTarGz(yearStr, monThStr, dayFlag)
        try {
            delDir(`${__dirname}/${dataLocation}/data/${yearStr}/${monThStr}/${dayFlag}/`, { recursive: true });
        } catch (error) {
            logger.error(error.message)
        }
    }
    if (stopFlag) {
        process.exit();
    }
    dayFlag = dayStr;
    let dirPath = `${__dirname}/${dataLocation}/data/${yearStr}/${monThStr}/${dayStr}`;
    for (let groupID = 0; groupID < cbGroup.length; groupID++) {
        const groupCb = cbGroup[groupID];
        createFolderIfNotExists(`${dirPath}/group_${groupID}`)
        if (intervalData) {
            for (let i = 0; i < intervalList_min.length; i++) {
                const element = intervalList_min[i];
                createFolderIfNotExists(`${dirPath}/group_${groupID}/${element}_min`)
            }
        }
        fs.writeFileSync(`${dirPath}/group_${groupID}/export.sql`, "");
        for (let i = 0; i < groupCb.length; i++) {
            const tagCb = groupCb[i];
            const tagSaveTime = cacheTime.get(tagCb);
            try {
                // const tag = childTableMap.get(tagCb);
                // let hourStr = new Date(tagSaveTime).getHours();
                let csvStr = `${tagCb}`;
                const filePath = `${dirPath}/group_${groupID}/${csvStr}.csv`;
                let exportSql = `SELECT * FROM ${tagCb} WHERE DataTime >= '${ts_to_time(tagSaveTime)}' AND DataTime <= '${ts_to_time(tagSaveTime + frequencyTime)}'  >>  ${filePath}`;
                await writeDataToFile(`${dirPath}/group_${groupID}/export.sql`, exportSql + "\n");
                if (intervalData) {
                    for (let j = 0; j < intervalList_min.length; j++) {
                        const element = intervalList_min[j];
                        const filePath = `${dirPath}/group_${groupID}/${element}_min/${csvStr}.csv`;
                        let exportSql_min = `SELECT * FROM ${tagCb}_${element}min WHERE DataTime >= '${ts_to_time(tagSaveTime)}' AND DataTime <= '${ts_to_time(tagSaveTime + frequencyTime)}'  >>  ${filePath}`;
                        await writeDataToFile(`${dirPath}/group_${groupID}/${element}_min/export.sql`, exportSql_min + "\n");
                    }
                }
            } catch (error) {
                logger.error(error)
            }
            cacheTime.set(tagCb, tagSaveTime + frequencyTime);
        }

        masterDBCliClass.sourceSqlFile(dbInfo.database, `${dirPath}/group_${groupID}/export.sql`);
        if (intervalData) {
            for (let k = 0; k < intervalList_min.length; k++) {
                const element = intervalList_min[k];
                masterDBCliClass.sourceSqlFile(dbInfo.database, `${dirPath}/group_${groupID}/${element}_min/export.sql`);
            }
        }
    }
    logger.debug(ts_to_time(stopFlagTime) + "数据完成备份")
    stopFlagTime = stopFlagTime + frequencyTime;
    timer = setTimeout(async () => {
        await backup(cbGroup);
    }, 1000);
}

function createFolderIfNotExists(folderPath) {
    try {
        fs.statSync(folderPath);
        // logger.log('Folder already exists:', folderPath);
    } catch (err) {
        if (err.code === 'ENOENT') {
            // logger.log('Folder does not exist. Creating it now...', folderPath);
            fs.mkdirSync(folderPath, { recursive: true });
            // logger.log('Folder created:', folderPath);
        } else {
            logger.error(err.message);
        }
    }
}
async function mkTarGz(yearStr, monThStr, dayFlag) {
    return new Promise((resolve, reject) => {
        try {
            let oldDirPath = `${__dirname}/${dataLocation}/data/${yearStr}/${monThStr}`;
            const output = fs.createWriteStream(oldDirPath + `/${dayFlag}.tar.gz`);
            output.on('close', function () {
                // logger.log(archive.pointer() + ' total bytes');
                // logger.log('archiver has been finalized and the output file descriptor has closed.');
                resolve();
            });

            const archive = archiver('tar', {
                gzip: true,
                gzipOptions: {
                    level: 1
                }
            });
            archive.directory(oldDirPath + `/${dayFlag}`, `/${dayFlag}`);
            archive.pipe(output);
            archive.finalize();
            archive.on('error', function (err) {
                throw err;
            });
            archive.on('warning', function (err) {
                if (err.code === 'ENOENT') {
                    // log warning
                } else {
                    // throw error
                    throw err;
                }
            });
        } catch (error) {
            logger.error(error.message);
            resolve();
        }
    })

}
function delDir(path) {
    let files = [];
    if (fs.existsSync(path)) {
        files = fs.readdirSync(path);
        files.forEach((file, index) => {
            let curPath = path + "/" + file;
            if (fs.statSync(curPath).isDirectory()) {
                delDir(curPath); //递归删除文件夹
            } else {
                fs.unlinkSync(curPath); //删除文件
            }
        });
        fs.rmdirSync(path);
    }
}


// 子进程开始执行
async function start() {
    logger.info(`开始执行,进程ID:` + process.pid)
    checkTempFloder();
    await connectDB();
    await getChildTable();
    logger.info(`获取到表`);
    if (createTableFlag) {
        await createTable();
    }
    let cbGroup = group(_cbList, 1000);
    await writeGroupInfo(cbGroup);
    // // 开始同步
    await backup(cbGroup);
}

start();


