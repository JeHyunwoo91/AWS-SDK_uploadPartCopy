/**
 * Created by Mathias on 2017-03-08.
 */
var path = require('path'),
    async = require('async');

if (!process.env['NODE_CONFIG_DIR'])
    process.env['NODE_CONFIG_DIR'] = path.join(process.cwd(), '..', 'config');
process.chdir(path.join(process.cwd(), '..', 'src'));

var s3utils = require('../src/s3_uploadPartCopy');
var sourceBucketName = '/* source BucketName */';
var moveBucketName = '/* move BucketName */';

/*
* You can specify the file path or directory name as prefix.
* Let me give you some examples.
*
* case 1.
* If you want to copy only single file, for example 4GByte media.mp4 file in 'mathias/' directory
* then prefix will be 'mathias/media.mp4'.
*
* case 2.
* If you want to copy one or more files, then prefix will be 'media.mp4'
*
* */
var prefix = '/* specified sourcePath */';

async.waterfall([
    function (next) {
        s3utils.getAWSCrendential(function (err) {
            next(err);
        });
    },
    function (next) {
        s3utils.listObjects(sourceBucketName, prefix, function (err, list) {
            next(err, list);
        });
    },
    function (list, next) {
        s3utils.copy(sourceBucketName, list, moveBucketName, function (err) {
            next(err);
        });
    }
], function (err) {
    console.log('finish : ', err);
});