const request = require("request");
const typeEnum = {
  1: "bool",
  2: "int",
  3: "float",
  4: "double",
  5: "nchar"
};
class KIO {
  constructor() {}

  // encode(input){
  //   let output = Buffer.from(input).toString("base64");
  //   output = output.replace(/i/g,"ia");
  //   output = output.replace(/\+/g,"ib");
  //   output = output.replace(/\//g,"ic");
  //   output = output.replace(/\=/g,"");
  //   return output;
  // }
  getTagList(kioUrl) {
    try {
        let self = this;
      return new Promise((resolve) => {
        request(
          {
            url: `http://${kioUrl}/api/v1/variables`,
            method: "GET",
            headers: { "content-type": "application/json" },
            json: true,
          },
          function (err, res, body) {
            if (!err) {
              let tagInfoList = body.objectList;
              let tagList = [];
              for (let i = 0; i < tagInfoList.length; i++) {
                const tagInfo = tagInfoList[i];
                let tagName =  tagInfo.n;
                // let tagType  = typeEnum[tagInfo.t];
                // let childTableName = `${dbName}_${tagType}_${tagName}`;
                // let childTableEncodeName = self.encode(childTableName);
                tagList.push(tagName);
              }
              resolve(tagList)
            } else {
              resolve(null);
            }
          }
        );
      });
    } catch (error) {
      console.log(error);
      return null;
    }
  }

}
module.exports = new KIO();
