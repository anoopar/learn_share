"use strict";
var AWS = require("aws-sdk"),
    cfg = require('../../config.common').configData;

module.exports = class S3Access {

    constructor() {
        var config = {
            region: cfg.awsRegion,
            s3ForcePathStyle: true,
            endpoint: new AWS.Endpoint(cfg.s3Endpoint),
            // endpoint: deploy_endpoint
        };
        this._s3 = new AWS.S3(config);
    }

    listS3Objects(bucketName, path) {
        var params = {
            Bucket: bucketName,
            Delimiter: '/',
            Prefix: path
        };

        return this._s3.listObjects(params).promise()
            .then((data) => {

                console.log(data);
                return data.Contents;
            });
    }

    s3ObjectExists(bucketName, path, objectName) {
        return this.listS3Objects(bucketName, path)
            .then((objList) => {
                let names = objList.map((elem) => {
                    return elem.Key.split(path)[1];
                });
                return names.includes(objectName);
            });
    }

    listFiles(bucketName, path) {
        let dirPath = path.endsWith('/') ? path : (path + '/');
        return this.listS3Objects(bucketName, dirPath)
            .then((objList) => {
                return objList.map((elem) => elem.Key.split(dirPath)[1]);
            });
    }

    getChildFolders(bucketName, path) {
        var params = {
            Bucket: bucketName,
            Delimiter: '/',
            Prefix: path
        };
        return this._s3.listObjects(params).promise()
            .then((data) => {
                let arr = data.CommonPrefixes.map((elem) => elem.Prefix.split(path)[1]);
                console.log(arr);
                return arr;
            });
    }

    getFirstLevelChildFolders(bucketName, path) {
        return this.getChildFolders(bucketName, path)
            .then((folderList) => {
                let folders = folderList.map((elem) => {
                    let fold = elem.split('/');
                    let ret = (fold[0].length === 0 && fold.length > 2) ? fold[1] : fold[0];
                    return ret;
                });
                let folderSet = Array.from(new Set(folders));
                console.log(folderSet);
                return folderSet;
            });
    }

    writeNewObjectToS3(bucketName, pathKey, content) {
        var params = { Bucket: bucketName, Key: pathKey, Body: content };
        return this._s3.putObject(params).promise()
            .then(() => {
                return 'Success';
            });
    }

    readS3ObjectAsJson(bucketName, pathKey) {
        return this._s3.getObject({
            Bucket: bucketName,
            Key: pathKey
        }).promise()
            .then((data) => {
                return JSON.parse(data.Body.toString());
            });
    }

    readS3Object(bucketName, pathKey) {
        return this._s3.getObject({
            Bucket: bucketName,
            Key: pathKey
        }).promise()
            .then((data) => {
                return data.Body.toString();
            });
    }

    deleteS3Object(bucketName, pathKey) {
        return this._s3.deleteObject({
            Bucket: bucketName,
            Key: pathKey
        }).promise()
            .then((data) => {
                return 'Success';
            });
    }

    deleteS3Objects(bucketName, pathKeyArray) {
        let params = {
            Bucket: bucketName,
            Delete: {
                Objects: []
                // Quiet:
            }
        };

        if (pathKeyArray && pathKeyArray.length > 0) {
            params.Delete.Objects = pathKeyArray.map(elem => {
                return { Key: elem };
            });
            return this._s3.deleteObjects().promise()
                .then((data) => {
                    return 'Success';
                })
                .catch((err) => {
                    console.error('s3access: deleteS3Objects - Error during delete : ' + JSON.stringify(err));
                    throw err;
                });
        }
        return Promise.resolve(null);
    }
};
