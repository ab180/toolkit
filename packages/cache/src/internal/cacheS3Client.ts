import * as core from '@actions/core'
import * as crypto from 'crypto'
import * as fs from 'fs'
import {
  GetObjectCommand,
  HeadObjectCommand, ListObjectsCommand, ListObjectsV2Command,
  PutObjectCommand
} from "@aws-sdk/client-s3"

import {CompressionMethod} from './constants'
import {
  S3CacheEntry,
  InternalCacheOptions
} from './contracts'
import {isSuccessStatusCode} from './requestUtils'
import {s3Client} from "./s3Client"
import {Stream} from "stream"
import * as util from "util"
import * as stream from "stream"

const versionSalt = '1.0'

async function pipeResponseToStream(
  response: NodeJS.ReadableStream,
  output: NodeJS.WritableStream
): Promise<void> {
  const pipeline = util.promisify(stream.pipeline)
  await pipeline(response, output)
}

function createS3Client() {
  const bucket = process.env['ACTIONS_CACHE_S3_BUCKET'] || ''
  const prefix = process.env['ACTIONS_CACHE_S3_PREFIX'] || ''
  const delimiter = prefix.endsWith('/') ? '' : '/'
  return {
    findKey: async (key: string): Promise<{ objectKey: string } | null> => {
      const cmd = new ListObjectsV2Command({
        Bucket: bucket,
        Prefix: `${prefix}${delimiter}${key}`,
        MaxKeys: 1
      })
      core.debug(`s3Rquest: ${JSON.stringify(cmd)}`)
      const response = await s3Client.send(cmd)
      core.debug(`s3Response: ${JSON.stringify(response)}`)
      if (!!response.Contents && response.Contents.length > 0) {
        const s3Key = response.Contents[0].Key!!
        const objectKey = s3Key.replace(new RegExp(`^${prefix}${delimiter}`), "")
        return {objectKey}
      }
      return null
    },
    getObject: async (key: string, archivePath: string) => {
      const cmd = new GetObjectCommand({
        Bucket: bucket,
        Key: `${prefix}${delimiter}${key}`
      })
      core.debug(`s3Rquest: ${JSON.stringify(cmd)}`)
      const response = await s3Client.send(cmd)
      const body = response.Body

      const writeStream = fs.createWriteStream(archivePath)

      if (body instanceof Stream)
        await pipeResponseToStream(body, writeStream)
      else if (body instanceof ReadableStream)
        core.error("ReadableStream not supported")
      else if (body instanceof Blob)
        core.error("Blob not supported")

    },
    putObject: async (key: string, archivePath: string) => {
      const readStream = fs.createReadStream(archivePath)
      const cmd = new PutObjectCommand({
        Bucket: bucket,
        Key: `${prefix}${delimiter}${key}`,
        Body: readStream
      })
      const response = await s3Client.send(cmd)
      return response.$metadata.httpStatusCode
    }
  }
}

function getCacheVersion(
  paths: string[],
  compressionMethod?: CompressionMethod
): string {
  const components = paths.concat(
    !compressionMethod || compressionMethod === CompressionMethod.Gzip
      ? []
      : [compressionMethod]
  )

  // Add salt to cache version to support breaking changes in cache entry
  components.push(versionSalt)

  return crypto
    .createHash('sha256')
    .update(components.join('|'))
    .digest('hex')
}

export function createObjectKey(key: string,
                paths: string[],
                options?: InternalCacheOptions): string {
  const version = getCacheVersion(paths, options?.compressionMethod)
  return `${version}/${key}`
}

export async function getCacheEntry(
  keys: string[],
  paths: string[],
  options?: InternalCacheOptions
): Promise<S3CacheEntry | null> {
  const s3Client = createS3Client()

  for (const key of keys) {
    const objectKey = createObjectKey(key, paths, options)
    const entity = await s3Client.findKey(objectKey)
    if (!!entity) {
      core.debug(`found cache with key : ${key}`)
      return {
        cacheKey: key,
        ...entity
      }
    }
  }
  return null
}

export async function downloadCache(
  objectKey: string,
  archivePath: string,
  options?: any
): Promise<void> {
  const s3client = createS3Client()
  await s3client.getObject(objectKey, archivePath)
}

export async function saveCache(
  objectKey: string,
  archivePath: string,
  options?: any
): Promise<void> {
  const s3client = createS3Client()

  core.debug('Upload cache')
  const responseCode = await s3client.putObject(objectKey, archivePath)
  if (!isSuccessStatusCode(responseCode)) {
    throw new Error(
      `Cache service responded with ${responseCode} during commit cache.`
    )
  }

  core.info('Cache saved successfully')
}
