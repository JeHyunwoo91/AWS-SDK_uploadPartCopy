/**
 * Created by Mathias on 2017-03-08.
 */
var path = require('path'),
    AWS =  require('aws-sdk'),
    config = require('config'),
    async = require('async');

const extType = [
    '.mp4',
    '.mpeg',
    '.mpg',
    '.mxf',
    '.avi',
    '.mkv',
    '.wmv',
    '.ts',
    '.m2ts',
    '.mov'
    /* you can specified any file extension you needed */
];

const maxRetries = 3;

var CopyObject = module.exports = {
    getAWSCrendential: function (callback) {
        if (config.paths.awsSetting.indexOf('/') === 0)
            AWS.config.loadFromPath(config.paths.awsSetting);
        else
            AWS.config.loadFromPath(path.join(process.cwd(), '..', config.paths.awsSetting));

        callback(null);
    },
    copy: function (copyBucket, copyPathList, pasteBucket, callback) {
        var error = null;
        var self = this;

        async.eachLimit(copyPathList, 5, function (filePath, next) {
            if (!filePath) {
                error = new Error('No Such File or Directory in ' + copyBucket);
                return callback(error);
            }

            var pastePath = path.join('sample', filePath.Key);
            var multipartParams = {
                Bucket: pasteBucket,
                Key: pastePath,
                ContentType: '/* specified contentType. for example, \'video/mp4\' */',
                CacheControl: "max-age=0, s-maxage=86400"
            };

            self.startMultipartCopy(multipartParams, copyBucket, filePath, pasteBucket, function (mpErr) {
                next(mpErr);
            });
        }, function (err) {
            callback(err);
        });
    },
    startMultipartCopy: function (multipartParams, copyBucket, copyPath, pasteBucket, callback) {
        var self = this;

        var numPartsLeft = {},
            lastPartNum,
            rangeStart = 0,
            partNum = 0,
            fileSizeInBytes = copyPath.Size,
            partSize = 1024 * 1024 * 200, // I think optimal execute speed when 200Mbyte chunkSize.
            multipartMap = {
                Parts: []
            };

        lastPartNum = Math.ceil(fileSizeInBytes / partSize);
        numPartsLeft.left = lastPartNum;

        console.log('filSize / lastPartNum : ', fileSizeInBytes + ' / ' + lastPartNum);

        var s3 = new AWS.S3();

        s3.createMultipartUpload(multipartParams, function (mpErr, multipart) {
            if (mpErr)
                return callback(mpErr);

            async.whilst(
                function () {
                    return rangeStart < fileSizeInBytes;
                },
                function (cb) {
                    var readBuff;
                    if ((partNum + 1) == lastPartNum)
                        readBuff = fileSizeInBytes - (partSize * partNum);
                    else
                        readBuff = partSize;

                    partNum++;

                    var partParams = {
                        Bucket: pasteBucket,
                        CopySource: path.join(copyBucket, copyPath.Key),
                        Key: multipartParams.Key,
                        PartNumber: String(partNum),
                        UploadId: multipart.UploadId,
                        CopySourceRange: 'bytes=' + rangeStart + '-' + (rangeStart + readBuff - 1)
                    };
                    console.log(partParams);
                    console.log('Got Upload ID(' + multipartParams.Key + ') : \n' + multipart.UploadId);

                    self.uploadPartCopy(multipart, partParams, multipartMap, numPartsLeft, function (mpErr) {
                        if (mpErr)
                            return cb(mpErr);

                        rangeStart += readBuff;
                        cb(null);
                    });
                }, function (err) {
                    callback(err);
                }
            );
        });
    },
    uploadPartCopy: function (multipart, partParams, multipartMap, numPartsLeft, callback, tryNum) {
        var self = this,
            _tryNum = tryNum || 1,
            error = null;

        var s3 = new AWS.S3();

        s3.uploadPartCopy(partParams, function (mpErr, mpData) {
            if (mpErr) {
                if (_tryNum < maxRetries) {
                    self.uploadPartCopy(multipart, partParams, multipartMap, numPartsLeft, callback, _tryNum + 1);
                }
                else {
                    error = new Error('Fail Copying Part #' + partParams.PartNumber + '\n' + mpErr);
                    return callback(error);
                }

                return;
            }

            multipartMap.Parts[this.request.params.PartNumber - 1] = {
                ETag: mpData.ETag,
                PartNumber: Number(this.request.params.PartNumber)
            };

            if (--numPartsLeft.left > 0) {
                console.log('numPartsLeft(' + partParams.Key + ') : ' + numPartsLeft.left)
                return callback(null);
            }

            console.log('Last Part of ' + partParams.Key);

            var doneParams = {
                Bucket: partParams.Bucket,
                Key: partParams.Key,
                MultipartUpload: multipartMap,
                UploadId: partParams.UploadId
            };

            self.completeMultipartUpload(doneParams, function (err) {
                callback(err);
            });
        });
    },
    completeMultipartUpload: function (doneParams, callback) {
        var s3 = new AWS.S3();

        s3.completeMultipartUpload(doneParams, function (mpErr, mpData) {
            if (mpErr)
                return callback(mpErr);

            console.log('Completed upload ' + doneParams.Key);
            console.log('Result of upload : \n' + JSON.stringify(mpData, null, 4));

            callback(mpErr);
        });
    },
    listObjects: function (bucketName, prefix, callback) {
        var list = [];

        var params = {
            Bucket: bucketName,
            Prefix: prefix
        };
        var s3 = new AWS.S3();

        s3.listObjects(params, function (err, data) {
            if (err)
                return callback(err);

            for (var file in data.Contents) {
                if (extType.indexOf(path.extname(data.Contents[file].Key)) != -1)
                    list.push({Key: data.Contents[file].Key, Size: data.Contents[file].Size});
            }

            callback(err, list);
        });
    }
};

