"use strict";
const request = require("request");
class KDBTDEngine {
  constructor({
    host: t = "127.0.0.1",
    port: e = 6041,
    user: s = "root",
    password: o = "taosdata",
    database: n = "",
  } = {}) {
    (this._host = t),
      (this._port = e),
      (this._user = s),
      (this._password = o),
      (this._database = n),
      (this._token = "");
  }
  async connect() {
    try {
      if (
        ((this._token = await _tdGetToken(
          this._host,
          this._port,
          this._user,
          this._password
        )),
          "" !== this._token && "" !== this._database)
      ) {
        return new Promise((t, e) => {
          t("open success");
        });
      }
    } catch (t) {
      return new Promise((e, s) => {
        s(t);
      });
    }
  }
  async close() {
    return (
      (this._host = ""),
      (this._port = ""),
      (this._user = ""),
      (this._password = ""),
      (this._database = ""),
      (this._token = ""),
      new Promise((t, e) => {
        t(!0);
      })
    );
  }
  async isOpen() {
    try {
      return "" !== this._token
        ? (await _tdExcuteSQL(
          this._host,
          this._port,
          this._token,
          this._database,
          "show databases;"
        ),
          new Promise((t, e) => {
            t(!0);
          }))
        : new Promise((t, e) => {
          e(new Error("token is null"));
        });
    } catch (t) {
      return new Promise((e, s) => {
        s(t);
      });
    }
  }
  async query(t) {
    if ("" !== this._token) {
      let e = this._database,
        s = await _tdExcuteSQL(this._host, this._port, this._token, e, t);
      return new Promise((t, e) => {
        t(s);
      });
    }
    return new Promise((t, e) => {
      e(new Error("token is null"));
    });
  }
}
async function _tdHttpGet(t) {
  let e = {
    url: t,
    method: "GET",
    headers: { "Content-Type": "application/json" },
  };
  return new Promise((t, s) => {
    request(e, function (e, o) {
      e ? s(e) : t(o);
    });
  });
}
async function _tdHttpPostBody(t, e, s) {
  let o = {
    url: t,
    method: "POST",
    headers: {
      Authorization: "Taosd " + e,
      "Content-Type": "application/json",
    },
    body: s,
  };
  return new Promise((t, e) => {
    request(o, function (s, o) {
      if (s) {
        console.error(s)
        t({ code: -1 });
      }
      else
        try {
          t(JSON.parse(o.body));
        } catch (s) {
          console.error(s)
          t({ code: -1 });
        }
    });
  });
}
async function _tdGetToken(t, e, s, o) {
  let n = "http://" + t + ":" + e + "/rest/login/" + s + "/" + o;
  return new Promise((t, e) => {
    _tdHttpGet(n)
      .then((s) => {
        try {
          let o = JSON.parse(s.body);
          0 === o.code ? t(o.desc) : e(o);
        } catch (t) {
          e(t);
        }
      })
      .catch((t) => {
        e(t);
      });
  });
}
async function _tdExcuteSQL(t, e, s, o, n) {
  let r = "http://" + t + ":" + e + "/rest/sql/" + o;
  return await _tdHttpPostBody(r, s, n);
}
module.exports = { KDBTDEngine: KDBTDEngine };
