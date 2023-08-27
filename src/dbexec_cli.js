const shell = require("shelljs");

class DBExec {
  constructor(ip, auth) {
    this.ip = ip;
    this.auth = auth;
    this.cliPort = 6050;
  }
  showAllDB() {
    let ret = shell.exec(
      `taos -P ${this.cliPort} -h ${this.ip} -a ${this.auth} -s "SHOW DATABASES;"`
    );
    return ret;
  }
  getDBCreate(database) {
    let commandStr = `taos -P ${this.cliPort} -h ${this.ip} -a ${this.auth} -s "SHOW CREATE DATABASE ${database} \\G;"`;
    let ret = shell.exec(commandStr);
    let result = "";
    if (!ret.stderr) {
      let stdout = ret.stdout;
      result = stdout.slice(
        stdout.indexOf("Create Database: ") + 17,
        stdout.indexOf("\r\nQuery OK")
      );
    } else {
      ret.code = ret.code ? ret.code : -1;
    }
    return ret.code
      ? { code: ret.code, stderr: ret.stderr, stdout: ret.stdout }
      : { code: ret.code, stderr: ret.stderr, stdout: result };
  }
  getStbCreate(database, stb_name) {
    let commandStr = `taos -P ${this.cliPort} -h ${this.ip} -a ${this.auth} -d ${database} -s "SHOW CREATE STABLE ${stb_name} \\G;"`;
    let ret = shell.exec(commandStr);
    let result = "";
    if (!ret.stderr) {
      // result = ret.stdout;
      let stdout = ret.stdout;
      result = stdout.slice(
        stdout.indexOf("Create Table: ") + 14,
        stdout.indexOf("\r\nQuery OK")
      );
    } else {
      ret.code = ret.code ? ret.code : -1;
    }
    return ret.code
      ? { code: ret.code, stderr: ret.stderr, stdout: ret.stdout }
      : { code: ret.code, stderr: ret.stderr, stdout: result };
  }

  getTbCreate(database, tb_name) {
    let commandStr = `taos -P ${this.cliPort} -h ${this.ip} -a ${this.auth} -d ${database} -s "SHOW CREATE TABLE ${tb_name} \\G;"`;
    let ret = shell.exec(commandStr);
    let result = "";
    if (!ret.stderr) {
      // result = ret.stdout;
      let stdout = ret.stdout;
      result = stdout.slice(
        stdout.indexOf("Create Table: ") + 14,
        stdout.indexOf("\r\nQuery OK")
      );
    } else {
      ret.code = ret.code ? ret.code : -1;
    }
    return ret.code
      ? { code: ret.code, stderr: ret.stderr, stdout: ret.stdout }
      : { code: ret.code, stderr: ret.stderr, stdout: result };
  }

  createDB(dbName) {
    let commandStr = `taos -P ${this.cliPort} -h ${this.ip} -a ${this.auth} -s "CREATE DATABASE ${dbName} CACHEMODEL 'last_row'"`;
    let ret = shell.exec(commandStr);
    let result = "";
    if (!ret.stderr) {
      result = ret.stdout;
      // let stdout = ret.stdout;
      // result = stdout.slice(
      //   stdout.indexOf("Create Table: ") + 14,
      //   stdout.indexOf("\r\nQuery OK")
      // );
    } else {
      ret.code = ret.code ? ret.code : -1;
    }
    return ret.code
      ? { code: ret.code, stderr: ret.stderr, stdout: ret.stdout }
      : { code: ret.code, stderr: ret.stderr, stdout: result };
  }
  createStb(database,createStbStr){
    let commandStr = `taos -P ${this.cliPort} -h ${this.ip} -a ${this.auth} -d ${database} -s "${createStbStr}"`;
    commandStr = this._replaceAll("`","",commandStr) 
    let ret = shell.exec(commandStr);
    let result = "";
    if (!ret.stderr) {
      result = ret.stdout;
      // let stdout = ret.stdout;
      // result = stdout.slice(
      //   stdout.indexOf("Create Table: ") + 14,
      //   stdout.indexOf("\r\nQuery OK")
      // );
    } else {
      ret.code = ret.code ? ret.code : -1;
    }
    return ret.code
      ? { code: ret.code, stderr: ret.stderr, stdout: ret.stdout }
      : { code: ret.code, stderr: ret.stderr, stdout: result };
  }
  createTb(database,createStbStr){
    let commandStr = this._replaceAll("`","",createStbStr) 
    commandStr = this._replaceAll(", ,",', " " ,',commandStr) 
    createStbStr = `taos -P ${this.cliPort} -h ${this.ip} -a ${this.auth} -d ${database} -s '${commandStr}'`;
    let ret = shell.exec(createStbStr);
    let result = "";
    if (!ret.stderr) {
      // result = ret.stdout;
      let stdout = ret.stdout;
      result = stdout.slice(
        stdout.indexOf("Create OK, ") + 11,
        stdout.indexOf(" row(s)")
      );
    } else {
      ret.code = ret.code ? ret.code : -1;
    }
    return ret.code
      ? { code: ret.code, stderr: ret.stderr, stdout: ret.stdout }
      : { code: ret.code, stderr: ret.stderr, stdout: result };
  }
  queryCountSql(database,sql){
    let commandStr = `taos -P ${this.cliPort} -h ${this.ip} -a ${this.auth} -d ${database} -s "${sql} \\G"`;
    let ret = shell.exec(commandStr);
    let result = "";
    if (!ret.stderr) {
      // result = ret.stdout;
      let stdout = ret.stdout;
      result = stdout.slice(
        stdout.indexOf("count(*): ") + 10,
        stdout.indexOf("\r\nQuery OK")
      );
    } else {
      ret.code = ret.code ? ret.code : -1;
    }
    return ret.code
      ? { code: ret.code, stderr: ret.stderr, stdout: ret.stdout }
      : { code: ret.code, stderr: ret.stderr, stdout: result };
  }
  queryChildTable(database,sql){
    let commandStr = `taos -P ${this.cliPort} -h ${this.ip} -a ${this.auth} -d ${database} -s "${sql} \\G"`;
    let ret = shell.exec(commandStr);
    let result = "";
    if (!ret.stderr) {
      // result = ret.stdout;
      let stdout = ret.stdout;
      result = stdout.slice(
        stdout.indexOf("Query OK, ") + 10,
        stdout.indexOf(" row(s)")
      );
    } else {
      ret.code = ret.code ? ret.code : -1;
    }
    return ret.code
      ? { code: ret.code, stderr: ret.stderr, stdout: ret.stdout }
      : { code: ret.code, stderr: ret.stderr, stdout: result };
  }
  exportData(database,sql,filePath){
    let commandStr = `taos -P ${this.cliPort} -h ${this.ip} -a ${this.auth} -d ${database} -s "${sql} >> ${filePath} \\G"`;
    let ret = shell.exec(commandStr);
    let result = "";
    if (!ret.stderr) {
      result = ret.stdout;
      // let stdout = ret.stdout;
      
    } else {
      ret.code = ret.code ? ret.code : -1;
    }
    return ret.code
      ? { code: ret.code, stderr: ret.stderr, stdout: ret.stdout }
      : { code: ret.code, stderr: ret.stderr, stdout: result };
  }

  importData(database,filePath,childTable){
    let commandStr = `taos -P ${this.cliPort} -h ${this.ip} -a ${this.auth} -d ${database} -s "insert into ${childTable} file '${filePath}'"`;
    let ret = shell.exec(commandStr);
    let result = "";
    if (!ret.stderr) {
      result = ret.stdout;
      // let stdout = ret.stdout;
      
    } else {
      ret.code = ret.code ? ret.code : -1;
    }
    return ret.code
      ? { code: ret.code, stderr: ret.stderr, stdout: ret.stdout }
      : { code: ret.code, stderr: ret.stderr, stdout: result };
  }

  sourceSqlFile(database,file){
    let commandStr = `taos -P ${this.cliPort} -h ${this.ip} -a ${this.auth} -d ${database} -f ${file}`;
    let ret = shell.exec(commandStr);
    let result = "";
    if (!ret.stderr) {
      result = ret.stdout;
    } else {
      ret.code = ret.code ? ret.code : -1;
    }
    return ret.code
      ? { code: ret.code, stderr: ret.stderr, stdout: ret.stdout }
      : { code: ret.code, stderr: ret.stderr, stdout: result };
  }



  _replaceAll(find, replace, str) {
    var find = find.replace(/[-\/\\^$*+?.()|[\]{}]/g, '\\$&');
    return str.replace(new RegExp(find, 'g'), replace);
  }
  static getInstance(ip, auth) {
    return new DBExec(ip, auth);
  }
}
module.exports = DBExec;
