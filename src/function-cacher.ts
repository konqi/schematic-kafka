import { createHash } from "crypto"

const hash = (obj: object) => {
  return createHash("sha1").update(JSON.stringify(obj)).digest("hex")
}

/**
 * Class that provides a caching wrapper for functions
 */
export class FunctionCacher {
  private cache: Record<string, any> = {}

  /**
   * Clear all cached values or a single cached value
   * @param key (optional) key to remove from the cache. If omitted the complete cache is evicted
   */
  clear(key?: string) {
    if (key) {
      delete this.cache[key]
    } else {
      this.cache = {}
    }
  }

  /**
   * Create a function proxy that caches the function's result.
   * The key for the cache value is generated from the function arguments.
   * The values that should be used for the cache key are provided as the second argument.
   * @param {function} functionToCache the function that will be wrapped
   * @param {[boolean]} useArgumentForCache an array of booleans the size of the function arguments. Each item represents the corresponding argument in the functionToCache.
   * @param context a context to run the functionToCache in. This is relevant if for example the function is a class method and it must have access to other class properties / methods.
   * @returns {function} function with the same signature as the functionToCache
   */
  createCachedFunction<FnToCacheResponse extends (...args: Array<any>) => ReturnType<FnToCacheResponse>>(
    functionToCache: FnToCacheResponse,
    useArgumentForCache: { [key in keyof FnToCacheResponse]: boolean },
    context?: any
  ) {
    const wrapper = (...args: Parameters<FnToCacheResponse>): ReturnType<FnToCacheResponse> => {
      let cacheKey = undefined
      // typesafe cast for what we know to be true
      if (Array.isArray(useArgumentForCache)) {
        const cacheKeySegments = (useArgumentForCache as Array<boolean>).map((value, index) => {
          switch (typeof args[index]) {
            case "string":
            case "number":
              return `${args[index]}`
            default:
              return hash(args[index])
          }
        })
        cacheKey = `${functionToCache.name}__${cacheKeySegments.join("__")}`
      } else {
        /* istanbul ignore next */
        throw new Error("This is impossble!")
      }

      // check cache hit
      if (this.cache[cacheKey]) {
        return this.cache[cacheKey]
      }

      let result = functionToCache.apply(context ?? null, args)

      if ((result as any) instanceof Promise) {
        result = (result as Promise<any>).catch((reason) => {
          // evict from cache
          this.clear(cacheKey)

          throw reason
        }) as ReturnType<FnToCacheResponse>
      }

      // put into cache
      if (result !== undefined) {
        this.cache[cacheKey] = result
      }

      return result
    }

    return wrapper
  }
}
