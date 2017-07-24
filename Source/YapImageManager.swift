//
//  YapImageManager.swift
//  YapImageManager
//
//  Created by Trevor Stout on 11/8/13.
//  Copyright (c) 2013-2017 Yap Studios LLC. All rights reserved.
//
//  Redistribution and use in source and binary forms, with or without
//  modification, are permitted provided that the following conditions are met:
//
//  * Redistributions of source code must retain the above copyright notice, this
//  list of conditions and the following disclaimer.
//
//  * Redistributions in binary form must reproduce the above copyright notice,
//  this list of conditions and the following disclaimer in the documentation
//  and/or other materials provided with the distribution.
//
//  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
//  AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
//  IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
//  DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
//  FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
//  DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
//  SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
//  CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
//  OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
//  OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

import YapDatabase
import Alamofire
import CocoaLumberjack

public let YapImageManagerUpdatedNotification = Notification.Name(rawValue: "YapImageManagerUpdatedNotification")
public let YapImageManagerFailedNotification = Notification.Name(rawValue: "YapImageManagerFailedNotification")
public let YapImageManagerImageWillBeginDownloadingNotification = Notification.Name(rawValue: "YapImageManagerImageWillBeginDownloadingNotification")
public let YapImageManagerImageAttributesUpdatedNotification = Notification.Name(rawValue: "YapImageManagerImageAttributesUpdatedNotification")
public let YapImageManagerURLKey: String = "image_url"
public let YapImageManagerImageAttributesKey: String = "image_attributes"
public let YapImageManagerImageCollection: String = "images"
public let YapImageManagerImageAttributesCollection: String = "image_attributes"

public struct YapImageManagerConfiguration {
  
  /// The name of the image database, excluding the path
  var databaseName: String
  /// The name of the image attributes database, excluding the path
  var attributesDatabaseName: String
  /// Time to expire image and remove from the database, in seconds
  var expireImagesAfter: TimeInterval
  /// Time to expire image attributes and remove from the database, in seconds. This should be >= expireImageAfter
  var expireImageAttributesAfter: TimeInterval
  /// When decoding images, if no size is specified decode up to max/width height
  var maxDecodeWidthHeight: Float
  /// When saving images to database, reduce the file size for any image greater than specified max width/height. Note: re-encoding to JPG is an expensive operation, so use a max width/height to limit conversions
  var maxSaveToDatabaseWidthHeight: Float
  /// When maxSaveToDatabaseWidthHeight is exceeded, resize to specified max width/height, preserving aspect ratio
  var widthToResizeIfDatabaseMaxWidthHeightExceeded: Float
  /// The number of simultaneous image requests
  var maxSimultaneousImageRequests: Int
  /// Timeout interval for image downloads
  var timeoutIntervalForRequest: TimeInterval
  /// An array of acceptable image content types, for example ["image/jpeg", "image/jpg", ...]
  var acceptableContentTypes: [String]
}

/// A unique ticket for an image request, used to cancel request
public typealias ImageRequestTicket = String

/// Closure when image request is completed
public typealias ImageRequestCompletion = (_ result: ImageResponse) -> Void

public enum ImageResponseCacheType {
  /// The image was downloaded, not returned from the memory or database cache
  case none
  /// The image was returned from the memory cache
  case cache
  /// The image was returned from the database cache
  case databaseCache
}

public class ImageResponse {
  
  /// The original requested URLString
  public var URLString: String
  /// The original ticket for the request
  public var ticket: ImageRequestTicket
  /// The image, or nil if there was an error
  public var image: UIImage?
  /// The cache type, used to indicate an image that was returned from memory cache, database cache, or downloaded
  public var cacheType: ImageResponseCacheType
  /// The error, if any, or nil
  public var error: Error?
  
  init(URLString: String, ticket: ImageRequestTicket, image: UIImage?, cacheType: ImageResponseCacheType = .none, error: Error? = nil) {
    self.URLString = URLString
    self.ticket = ticket
    self.image = image
    self.cacheType = cacheType
    self.error = error
  }
}

public enum YapImageManagerError: Error {
  
  /// Unable to decode image
  case imageDecodeFailure
}

private class ImageResponseHandler {
  
  var ticket: ImageRequestTicket
  var completion: ImageRequestCompletion
  var size: CGSize?
  var filters: [YapImageFilter]?
  
  init(ticket: ImageRequestTicket, completion: @escaping ImageRequestCompletion, size: CGSize? = nil, filters: [YapImageFilter]?) {
    self.ticket = ticket
    self.completion = completion
    self.size = size
    self.filters = filters
  }
}

private class ImageRequest: NSObject {
  
  var URLString: String = ""
  var progress: Progress?
  var downloadstartTime: Date? // start of download
  var responses = [ImageResponseHandler]()
  
  override var description: String {
    return URLString
  }
}

public class YapImageManager {

  // Configuration
  private var configuration: YapImageManagerConfiguration

  // Alamofire
  private var sessionManager: SessionManager
  private var reachabilityManager = NetworkReachabilityManager()
  
  // Database and Connections
  private var database: YapDatabase
  private var databaseConnection: YapDatabaseConnection
  private var backgroundDatabaseConnection: YapDatabaseConnection
  private var attributesDatabase: YapDatabase
  private var attributesDatabaseConnection: YapDatabaseConnection
  private var backgroundAttributesDatabaseConnection: YapDatabaseConnection
  
  // Queues and caches
  private var queuedRequests = [ImageRequest]()
  private var activeRequests = [ImageRequest]()
  private var pendingWritesDict = [AnyHashable: Any]()
  private var imageDecodeQueue: DispatchQueue
  private var attributesCache: NSCache<NSString, NSDictionary>?
  private var imagesCache: NSCache<NSString, UIImage>?
  private var isReachable: Bool = false

  // MARK: Initialization
  
  /// The default instance of YapImageManager
  static public let sharedInstance = YapImageManager()
  
  /// Returns the default YapImageManager configuration
  public class func defaultConfiguration() -> YapImageManagerConfiguration {
    
    let configuration = YapImageManagerConfiguration(
      databaseName: "YapImageManager.sqlite",
      attributesDatabaseName: "YapImageAttributes.sqlite",
      expireImagesAfter: 172800.0, // 48 hours
      expireImageAttributesAfter: 1209600.0, // 2 weeks
      maxDecodeWidthHeight: 1024.0,
      maxSaveToDatabaseWidthHeight: 4096.0,
      widthToResizeIfDatabaseMaxWidthHeightExceeded: 1024.0,
      maxSimultaneousImageRequests: 4,
      timeoutIntervalForRequest: 30.0,
      acceptableContentTypes: ["image/tiff", "image/jpeg", "image/jpg", "image/gif", "image/png", "image/ico", "image/x-icon", "image/bmp", "image/x-bmp", "image/x-xbitmap", "image/x-win-bitmap"]
    )
    return configuration
  }
  
  /// Initializes YapImageManager with the given configuration
  ///
  /// - parameter configuration: 'YapImageManagerConfiguration' used to create the instance
  ///
  /// - returns: The new 'YapImageManager' instance.
  init(configuration: YapImageManagerConfiguration = YapImageManager.defaultConfiguration()) {
		
    func databasePath(withName name: String) -> String {
			let paths = NSSearchPathForDirectoriesInDomains(.documentDirectory, .userDomainMask, true)
			let databaseDir = (paths.count > 0) ? (paths[0]) : NSTemporaryDirectory()
			return databaseDir + "/" + name
    }
    
    self.configuration = configuration

    // Create a session configuration with no cache
    let config = URLSessionConfiguration.ephemeral
    // Note: this is not the same a max concurrent operations
    config.httpMaximumConnectionsPerHost = configuration.maxSimultaneousImageRequests
    config.timeoutIntervalForRequest = configuration.timeoutIntervalForRequest
    sessionManager = SessionManager(configuration: config)

    // Initialize database and connections
    let options = YapDatabaseOptions()
    options.pragmaPageSize = 32768
    options.aggressiveWALTruncationSize = 1024 * 1024 * 100
    database = YapDatabase(path: databasePath(withName:configuration.databaseName), objectSerializer: nil, objectDeserializer: nil, metadataSerializer: nil, metadataDeserializer: nil, objectPreSanitizer: nil, objectPostSanitizer: nil, metadataPreSanitizer: nil, metadataPostSanitizer: nil, options: options)
    
    databaseConnection = database.newConnection()
    databaseConnection.objectCacheEnabled = false
    databaseConnection.metadataCacheEnabled = false
    databaseConnection.objectPolicy = .share
    databaseConnection.metadataPolicy = .share
    
    backgroundDatabaseConnection = database.newConnection()
    backgroundDatabaseConnection.objectCacheEnabled = false
    backgroundDatabaseConnection.metadataCacheEnabled = false
    backgroundDatabaseConnection.objectPolicy = .share
    backgroundDatabaseConnection.metadataPolicy = .share
    
    attributesDatabase = YapDatabase(path: databasePath(withName: configuration.attributesDatabaseName))
    attributesDatabaseConnection = attributesDatabase.newConnection()
    attributesDatabaseConnection.objectCacheEnabled = false
    attributesDatabaseConnection.metadataCacheEnabled = false
    attributesDatabaseConnection.objectPolicy = .share
    attributesDatabaseConnection.metadataPolicy = .share

    backgroundAttributesDatabaseConnection = attributesDatabase.newConnection()
    backgroundAttributesDatabaseConnection.objectCacheEnabled = false
    backgroundAttributesDatabaseConnection.metadataCacheEnabled = false
    backgroundAttributesDatabaseConnection.objectPolicy = .share
    backgroundAttributesDatabaseConnection.metadataPolicy = .share

    imageDecodeQueue = DispatchQueue(label: "YapImageManager.imageDecodeQueue", qos: .default, attributes: .concurrent)

    pendingWritesDict = [AnyHashable: Any]()
    attributesCache = NSCache()
    attributesCache?.countLimit = 1000
    imagesCache = NSCache()
    removeExpiredImages()
    vacuumDatabaseIfNeeded()

    isReachable = true
    reachabilityManager?.listener = { [weak self] reachable in
      let isReachable = (reachable == .reachable(.wwan) || reachable == .reachable(.ethernetOrWiFi))
      self?.isReachable = isReachable
      
      if isReachable {
        self?.processImageQueue()
      }
    }
    reachabilityManager?.startListening()
  }

  // MARK: Public methods

  /// Asynchronously requests an image for a given URLString. The image will be resized not to exceed ‘size’, if specified.
  /// Otherwise, the image will be decoded up to ‘maxDecodeWidthHeight’ while preserving aspect ratio. The downloaded and
  /// decoded image will be processed to apply any ‘filters’ specified. Image loading, decoding, and filtering is handled on
  /// a background queue.
  ///
  /// Images not available in the database or memory cache will be added to the front of the queue (lifo), normally
  /// corresponding to a user visible image. However, you can move any request to the front of the queue by calling
  /// prioritizeImageRequest(). You can also cancel any request no longer needed using cancelImageRequest(). Images will
  /// only be downloaded once regardless of the number of simultaneous requests.
  ///
  /// Once an image is downloaded, the original compressed image is stored in the database for fast retrieval. Images
  /// exceeding ‘maxSaveToDatabaseWidthHeight’ will be reduced to ‘widthToResizeIfDatabaseMaxWidthHeightExceeded’ before
  /// saving to the database. Additionally, decoded and filtered images are stored in a memory cache to improve scrolling
  /// performance.
  ///
  /// - parameter forURLString:  The URL string for the request.
  /// - parameter size:         The maximum image size to return. Defaults to ‘nil’.
  /// - parameter filters:      An array of ‘YapImageFilter’ for any post-processing. Defaults to ‘nil’.
  /// - parameter completion:   The closure to call when the download is complete.
  ///
  /// - returns: The 'ImageRequestTicket’ for the request.
  @discardableResult
  public func asyncImage(forURLString URLString: String, size: CGSize? = nil, filters: [YapImageFilter]? = [YapAspectFillFilter()], completion: @escaping ImageRequestCompletion) -> ImageRequestTicket {
		let ticket: ImageRequestTicket = UUID().uuidString
		let key = self.keyForImage(withURLString: URLString, size: size, filters: filters)
		
		// return cached image if available
    if let cachedImage = imagesCache?.object(forKey: key as NSString) {
      DDLogDebug("Found decoded image in memory cache \(URLString) key=\(key)")
			DispatchQueue.main.async(execute: {() -> Void in
        completion(ImageResponse(URLString: URLString, ticket: ticket, image: cachedImage, cacheType: .cache))
			})
    } else {
      
      // check the pending writes cache
      var imageData = pendingWritesDict[keyForImage(withURLString: URLString)] as? Data
      if imageData != nil {
        DDLogDebug("Found image in pending write cache \(URLString)")
      }
      
      imageDecodeQueue.async(execute: {() -> Void in
        if imageData == nil {
          // check database
          self.databaseConnection.read { transaction in
            imageData = transaction.object(forKey: self.keyForImage(withURLString: URLString), inCollection: YapImageManagerImageCollection) as? Data
            if imageData != nil {
              DDLogVerbose("Found image in database \(URLString)")
            }
          }
        }
        
        var image: UIImage?
        if let imageData = imageData {
          image = UIImage(data: imageData, scale: UIScreen.main.scale)
        }
        // if the database image was found, apply filters, save to memory cache, and return
        if let image = image {
          
          let filteredImage = self.filteredImage(forImage: image, resizedTo: size, filters: filters)
          
          if let filteredImage = filteredImage {
            // save image in cache
            DDLogVerbose("Save filtered image to memory cache \(URLString) key=\(key)")
            self.imagesCache?.setObject(filteredImage, forKey: key as NSString)
          }
          
          DispatchQueue.main.async(execute: {() -> Void in
            if let filteredImage = filteredImage {
              completion(ImageResponse(URLString: URLString, ticket: ticket, image: filteredImage, cacheType: .databaseCache))
            } else {
              DDLogError("Failed to filter image with URL \(URLString)")
            }
          })
        } else {
          DispatchQueue.main.async(execute: {() -> Void in
            // add to queue to download
            let response = ImageResponseHandler(
              ticket: ticket,
              completion: completion,
              size: size,
              filters: filters
            )
            self.queueImage(forURLString: URLString, response: response)
          })
        }
      })
    }
		return ticket
  }
	
  /// Cancels the request and removes any response handlers. If the image is queued for download it will be removed. Active
  /// requests are allowed to complete, however the response handler will not be called.
  ///
  /// - parameter forTicket: The ‘ImageRequestTicket’ to cancel.
  public func cancelImageRequest(forTicket ticket: ImageRequestTicket) {
		
		let currentCount = queuedRequests.count

		for request in queuedRequests {
			request.responses = request.responses.filter { $0.ticket != ticket }
		}
		
		// remove all requests without responses, unless request is active
		self.queuedRequests = queuedRequests.filter { !$0.responses.isEmpty || activeRequests.contains($0) }

		let newCount = queuedRequests.count
		if newCount < currentCount {
			DDLogVerbose("Cancelled queued image request with ticket \(ticket)")
		}
	}
	
  /// Asynchronously creates an image with the specified size and filters on a background queue. Useful for auto generating 
  /// and caching gradients and image placeholders.
  ///
  /// - parameter withSize:    The size of the image to return.
  /// - parameter filters:     An array of ‘YapImageFilter’ for processing the image.
  /// - parameter completion:  The closure to call when the image is complete
  public func createImage(withSize size: CGSize, filters: [YapImageFilter], completion: @escaping (_ image: UIImage?) -> Void) {
    
    let key = keyForImage(withURLString: nil, size: size, filters: filters)
    
    // Return cached image if available
    if let cachedImage = imagesCache?.object(forKey: key as NSString) {
      DDLogVerbose("Found filtered image in memory cache key=\(key)")
      completion(cachedImage)
      return
    }
    // Render image using filters
    imageDecodeQueue.async(execute: {() -> Void in
      if let filteredImage = self.filteredImage(forImage: nil, size: size, filters: filters) {
        // save image in cache
        self.imagesCache?.setObject(filteredImage, forKey: key as NSString)
        DispatchQueue.main.async(execute: {() -> Void in
          completion(filteredImage)
        })
      } else {
        DDLogError("Failed to create image")
        completion(nil)
      }
    })
  }

  /// Moves any pending request for URLString to the front of the download queue.
  ///
  /// - parameter forURLtring:  The URL string for the request.
  public func prioritizeImageRequest(forURLString URLString: String) {

    if let foundItem = queuedImageRequest(forURLString: URLString), let index = queuedRequests.index(of: foundItem) {
      // move to end of list to bump priority, if not already downloading
      queuedRequests.remove(at: index)
      queuedRequests.append(foundItem)
      DDLogVerbose("Increasing priority for download \(URLString)")
    }
  }
  
  /// Moves any pending request for URLString to the back of the download queue.
  ///
  /// - parameter forURLtring:  The URL string for the request.
  public func deprioritizeImageRequest(forURLString URLString: String) {
    
    if let foundItem = queuedImageRequest(forURLString: URLString), let index = queuedRequests.index(of: foundItem) {
      // move to end of list to bump priority, if not already downloading
      queuedRequests.remove(at: index)
      queuedRequests.insert(foundItem, at: 0)
      DDLogVerbose("Decreasing priority for download \(URLString)")
    }
  }
  
  /// Returns the fully decoded and filtered image from the memory cache, or nil if not available. The method is safe to use
  /// on the main thread while scrolling.
  ///
  /// - parameter forURLtring:  The URL string for the request.
  /// - parameter size:         The maximum image size to return. Defaults to ‘nil’.
  /// - parameter filters:      An array of ‘YapImageFilter’ for any post-processing. Defaults to ‘nil’.
  ///
  /// - returns: The ‘UIImage’ stored in the memory cache, or nil if not available
  public func cachedImage(forURLString URLString: String, size: CGSize? = nil, filters: [YapImageFilter]? = [YapAspectFillFilter()]) -> UIImage? {

    let key = keyForImage(withURLString: URLString, size: size, filters: filters)
    
    if let cachedImage = imagesCache?.object(forKey: key as NSString) {
      return cachedImage
    }
    return nil
  }

  /// Returns the original image cached in the database, or nil if not available. This method is synchronous and does not 
  /// return a decoded image. For best performance, do not use this while scrolling images on the main thread. Use 
  /// asyncImage() instead to avoid stuttering.
  ///
  /// - parameter forURLString: The URL string for the image
  ///
  /// - returns: The ‘UIImage’ stored in the database, or nil if not available
  public func databaseImage(forURLString URLString: String) -> UIImage? {
    if let imageData = databaseImageData(forURLString: URLString) {
      return UIImage(data: imageData, scale: UIScreen.main.scale)
    }
    return nil
  }
  
  /// Returns the original image data cached in the database, or nil if not available. For best performance, do not use this
  /// while scrolling on the main thread.
  ///
  /// - parameter forURLString: The URL string for the image
  ///
  /// - returns: The ‘Data’ stored in the database, or nil if not available
  public func databaseImageData(forURLString URLString: String) -> Data? {
    // check the database
    var imageData: Data? = nil
    databaseConnection.read { transaction in
      imageData = transaction.object(forKey: self.keyForImage(withURLString: URLString), inCollection: YapImageManagerImageCollection) as? Data
    }
    return imageData
  }

  /// Returns the ‘Progress’ for an active download request, or nil if not available. Progress is available after a 
  /// notification has been posted with the name ‘YapImageManagerImageWillBeginDownloadingNotification’
  ///
  /// - parameter forURLString: The URL string for the image
  ///
  /// - returns: The ‘Progress’ for the download request, or nil if not available.
  public func downloadProgress(forURLString URLString: String) -> Progress? {
    if let imageRequest = self.activeImageRequest(forURLString: URLString) {
      return imageRequest.progress
    } else {
      return nil
    }
  }
  
  // MARK: Image queue

  private func queueImage(forURLString URLString: String, response: ImageResponseHandler) {
    
    if let foundItem = queuedImageRequest(forURLString: URLString), let index = queuedRequests.index(of: foundItem) {
      // move to end of list to bump priority, if not already downloading
			foundItem.responses.append(response)
      queuedRequests.remove(at: index)
      queuedRequests.append(foundItem)
      DDLogVerbose("Requested image is already queued; increasing priority \(URLString)")
    } else {
      let request = ImageRequest()
      request.URLString = URLString
      request.responses.append(response)
      queuedRequests.append(request)
      DDLogVerbose("Queue image for download \(URLString)")
    }
    processImageQueue()
  }
  
  private func isImageQueued(forURLString URLString: String) -> Bool {
    
    // NOTE: Active imageRequests remain on downloadQueue, so it's only necessary to check downloadQueue
    let foundItem: ImageRequest? = queuedImageRequest(forURLString: URLString)
    return (foundItem != nil)
  }
  
  private func queuedImageRequest(forURLString URLString: String) -> ImageRequest? {
    
    let items = queuedRequests.filter { $0.URLString == URLString }
    return items.first
  }
  
  private func removeQueuedImageRequest(forURLString URLString: String) {
    
    if let foundItem = queuedImageRequest(forURLString: URLString), let index = queuedRequests.index(of: foundItem) {
      queuedRequests.remove(at: index)
    }
  }
  
  private func nextImageRequest() -> ImageRequest? {
    
    if let nextRequest = queuedRequests.last {
      if let _ = activeImageRequest(forURLString: nextRequest.URLString) {
        // skip
      } else {
        return nextRequest
      }
    }
    return nil
  }
	
	private func activeImageRequest(forURLString URLString: String) -> ImageRequest? {
    
		let requests = activeRequests.filter { $0.URLString == URLString }
		return requests.first
	}
	
	/// Returns true if active requests have not exceeded max simultaneous requests
	private func isReadyForRequest() -> Bool {
		return ((activeRequests.count < configuration.maxSimultaneousImageRequests) && isReachable)
	}
	
  private func processImageQueue() {
    
    guard isReadyForRequest()  else { return }
    
    // process image
    if !queuedRequests.isEmpty {
      if let imageRequest = nextImageRequest() {
        activeRequests.append(imageRequest)
        // start image download
        downloadImage(forRequest: imageRequest)
        DDLogVerbose("Download image \(imageRequest.URLString) (active:\(activeRequests.count) queued: \(queuedRequests.count))")
      }
    }
  }

  // MARK: - Image download

  private func downloadImage(forRequest imageRequest: ImageRequest) {
    
    let request = sessionManager.request(imageRequest.URLString)
      .validate(contentType: configuration.acceptableContentTypes)
      .responseData() { response in
        switch response.result {
        case .success:
          var imageData = response.data
          if imageData != nil {
            
            if let downloadstartTime = imageRequest.downloadstartTime {
              DDLogVerbose(String(format: "Image downloaded in %2.2f seconds %@", -downloadstartTime.timeIntervalSinceNow, imageRequest.URLString))
            }
            
            self.imageDecodeQueue.async(execute: {() -> Void in
              // update the image attributes, if necessary
              let image = UIImage(data: imageData!)
              let imageAttributes = self.imageAttributes(forURLString: imageRequest.URLString)
              var imageSize = CGSize.zero
              var shouldUpdateImageAttributes = false
              
              if let imageAttributes = imageAttributes {
                imageSize = self.imageSize(withAttributes: imageAttributes)
                
              } else if let image = image {
                // update image size attribute with actual image size; this should only be required if we were unable to pick up the image dimensions from the headers during download
                let scale: CGFloat = UIScreen.main.scale
                imageSize = CGSize(width: image.size.width * scale, height: image.size.height * scale)
                shouldUpdateImageAttributes = true
              }
              
              // Resize image to max database size, if necessary
              if let _ = self.maxSizeForImage(withSize: imageSize, maxWidthHeight: CGFloat(self.configuration.maxSaveToDatabaseWidthHeight)) {
                imageSize = self.maxSizeForImage(withSize: imageSize, maxWidthHeight: CGFloat(self.configuration.widthToResizeIfDatabaseMaxWidthHeightExceeded)) ?? imageSize
                DDLogVerbose("Image exceeded max size for database; resizing \(imageRequest.URLString)");
                if let resizedImage = self.filteredImage(forImage: image, size: imageSize) {
                  shouldUpdateImageAttributes = true
                  imageData = UIImageJPEGRepresentation(resizedImage, 0.8)
                }
              }
              
              // write the image attributes, if necessary
              if shouldUpdateImageAttributes {
                DDLogVerbose("Update image attributes \(imageRequest.URLString)")
                self.updateImageAttributes(with: imageSize, forURLString: imageRequest.URLString)
              }
              
              // decode all images and save to cache
              if let image = image {
                for completion in imageRequest.responses {
                  let filteredImage = self.filteredImage(forImage: image, resizedTo: completion.size, filters: completion.filters)
                  
                  if let filteredImage = filteredImage {
                    // save image in cache
                    let key = self.keyForImage(withURLString: imageRequest.URLString, size: completion.size, filters: completion.filters)
                    DDLogVerbose("Save filtered image to memory cache \(imageRequest.URLString) key=\(key)")
                    self.imagesCache?.setObject(filteredImage, forKey: key as NSString)
                  }
                }
              }
              
              // dispatch next request on main thread
              DispatchQueue.main.async(execute: {() -> Void in
                // remove queue item
                self.removeQueuedImageRequest(forURLString: imageRequest.URLString)
                imageRequest.progress = nil
                self.activeRequests.remove(at: self.activeRequests.index(of: imageRequest)!)
                if let imageData = imageData {
                  self.saveImageToDatabase(imageData, forURLString: imageRequest.URLString)
                }
                
                // call completion blocks
                for imageResponse in imageRequest.responses {
                  
                  let key = self.keyForImage(withURLString: imageRequest.URLString, size: imageResponse.size, filters: imageResponse.filters)
                  if let cachedImage = self.imagesCache?.object(forKey: key as NSString) {
                    DDLogDebug("Return decoded image after download \(imageRequest.URLString) key=\(key)")
                    imageResponse.completion(ImageResponse(URLString: imageRequest.URLString, ticket: imageResponse.ticket, image: cachedImage))
                  } else {
                    // An error occurred
                    imageResponse.completion(ImageResponse(URLString: imageRequest.URLString, ticket: imageResponse.ticket, image: nil, error: YapImageManagerError.imageDecodeFailure))
                  }
                }
                
                // POST notification
                let userInfo: [AnyHashable: Any] = [YapImageManagerURLKey: imageRequest.URLString]
                NotificationCenter.default.post(name: YapImageManagerUpdatedNotification, object: self, userInfo: userInfo)
                
                self.processImageQueue()
              })
            })
          } else {
            // TODO: write NULL to database to prevent repetitive download requests
          }
        case .failure(let error):
          // remove queue item
          self.removeQueuedImageRequest(forURLString: imageRequest.URLString)
          imageRequest.progress = nil
          self.activeRequests.remove(at: self.activeRequests.index(of: imageRequest)!)
          
          // call failed completion blocks
          for imageResponse in imageRequest.responses {
            // An error occurred
            imageResponse.completion(ImageResponse(URLString: imageRequest.URLString, ticket: imageResponse.ticket, image: nil, error: error))
          }
          
          // POST failed notification
          let userInfo: [AnyHashable: Any] = [YapImageManagerURLKey: imageRequest.URLString]
          NotificationCenter.default.post(name: YapImageManagerFailedNotification, object: self, userInfo: userInfo)
          DDLogError("Error downloading \(imageRequest.URLString) - \(error.localizedDescription)")
          
          // process next download
          self.processImageQueue()
        }
    }
    imageRequest.progress = request.progress
    imageRequest.downloadstartTime = Date()
    
    let userInfo: [AnyHashable: Any] = [YapImageManagerURLKey: imageRequest.URLString]
    NotificationCenter.default.post(name: YapImageManagerImageWillBeginDownloadingNotification, object: self, userInfo: userInfo)
  }
  
  private func saveImageToDatabase(_ imageData: Data, forURLString URLString: String) {
    
    let downloadTimestamp = Date()
    pendingWritesDict[keyForImage(withURLString: URLString)] = imageData
    
    backgroundDatabaseConnection.asyncReadWrite ({ transaction in
      transaction.setObject(imageData, forKey: self.keyForImage(withURLString: URLString), inCollection: YapImageManagerImageCollection, withMetadata: downloadTimestamp)
    }, completionBlock: { () -> Void in
      self.pendingWritesDict.removeValue(forKey: self.keyForImage(withURLString: URLString))
      DDLogVerbose("Save image to database \(URLString)")
    })
  }

  /// Returns a unique key for the image request, with a hash of the url, size, and all image filters
  func keyForImage(withURLString URLString: String?, size: CGSize? = nil, filters: [YapImageFilter]? = nil) -> String {
    var key = String()
    key += String(format: "\(URLString ?? "<na>")_%0.5f_%0.5f", size?.width ?? 0.0, size?.height ?? 0.0)
    
    if let filters = filters {
      for filter in filters {
        key += filter.key
      }
    }
    return key.md5Hash()
  }
  
  // MARK: Image filters
  
  private func filteredImage(forImage image: UIImage, resizedTo size: CGSize? = nil, filters: [YapImageFilter]? = nil) -> UIImage? {
    
    var filteredImage: UIImage?
    
    if let size = size {
      // resize image to size specified
      filteredImage = self.filteredImage(forImage: image, size: size, filters: filters)
    }	else {
      // otherwise, decode up to a max size
      if let maxSize = self.maxSizeForImage(withSize: image.size, maxWidthHeight: CGFloat(configuration.maxDecodeWidthHeight)) {
        filteredImage = self.filteredImage(forImage: image, size: maxSize, filters: filters)
      } else {
        filteredImage = self.filteredImage(forImage: image, size: image.size, filters: filters)
      }
    }
    return filteredImage
  }
  
  private func filteredImage(forImage image: UIImage?, size: CGSize, filters: [YapImageFilter]? = nil) -> UIImage? {
    
    var aspectFillImage: UIImage?
    UIGraphicsBeginImageContextWithOptions(size, false, UIScreen.main.scale)
    let imageRect = CGRect(x: 0, y: 0, width: size.width, height: size.height)
    guard let context = UIGraphicsGetCurrentContext() else { return nil }
    context.translateBy(x: 0.0, y: size.height)
    context.scaleBy(x: 1.0, y: -1.0)
    context.setBlendMode(.normal)
    context.interpolationQuality = .default
    
    if let filters = filters, !filters.isEmpty {
      for filter in filters {
        filter.draw(inContext: context, image: image, rect: imageRect, imageRect: imageRect)
      }
    } else {
      // if no filters were specifed, just render the image aspect fill
      let filter = YapAspectFillFilter()
      filter.draw(inContext: context, image: image, rect: imageRect, imageRect: imageRect)
    }
    
    // capture the image
    aspectFillImage = UIGraphicsGetImageFromCurrentImageContext()
    UIGraphicsEndImageContext()
    return aspectFillImage
  }
  
  // return nil if image does not need resizing, otherwise a new size with same aspect
  func maxSizeForImage(withSize imageSize: CGSize, maxWidthHeight: CGFloat) -> CGSize? {
    
    var maxSize: CGSize?
    if imageSize.width > maxWidthHeight || imageSize.height > maxWidthHeight {
      if imageSize.width > imageSize.height {
        maxSize = CGSize(
          width: maxWidthHeight,
          height: (maxWidthHeight * imageSize.height / imageSize.width).rounded()
        )
      }
      else {
        maxSize = CGSize(
          width: (maxWidthHeight * imageSize.width / imageSize.height).rounded(),
          height: maxWidthHeight
        )
      }
    }
    return maxSize
  }
  
  // MARK: - Image attributes
  
  /// NOTE: this feature is still in being ported from the Objective C version and will be made a public.
  /// The image size and width is decoded during download from the headers of jpeg, gif, and png files and
  /// a notification is posted. This is extremely useful for applications that need the image size for 
  /// layout in advance of the image download.

  /// Returns image attributes
  func imageAttributes(forURLString URLString: String) -> NSDictionary? {
    // check the database
    var imageAttributes: NSDictionary?
    imageAttributes = attributesCache?.object(forKey: URLString as NSString)
    if imageAttributes == nil {
      // check database
      if !isImageQueued(forURLString: URLString) {
        attributesDatabaseConnection.read { transaction in
          imageAttributes = transaction.object(forKey: URLString, inCollection: YapImageManagerImageAttributesCollection) as? NSDictionary
        }
        if let imageAttributes = imageAttributes {
          // save in cache
          attributesCache?.setObject(imageAttributes, forKey: URLString as NSString)
        }
      }
    }
    return imageAttributes
  }
  
  func imageSize(withAttributes imageAttributes: NSDictionary) -> CGSize {
    var imageSize = CGSize.zero
    if let width = (imageAttributes["image_width"] as? NSNumber), let height = (imageAttributes["image_height"] as? NSNumber) {
      imageSize = CGSize()
      imageSize.width = CGFloat(width.floatValue)
      imageSize.height = CGFloat(height.floatValue)
    }
    return imageSize
  }

  func postImageAttributesNotification(_ imageAttributes: NSDictionary, forURLString URLString: String) {
    let block: () -> Void = { () -> Void in
      let attributes: [AnyHashable: Any] = [YapImageManagerURLKey: URLString, YapImageManagerImageAttributesKey: imageAttributes]
      NotificationCenter.default.post(name: YapImageManagerImageAttributesUpdatedNotification, object: self, userInfo: attributes)
    }
    if Thread.isMainThread {
      block()
    }
    else {
      DispatchQueue.main.async(execute: block)
    }
  }
  
  func setImageAttributes(_ imageAttributes: NSDictionary, forURLString URLString: String) {
    attributesCache?.setObject(imageAttributes, forKey: URLString as NSString)
    postImageAttributesNotification(imageAttributes, forURLString: URLString)
    // save to database
    let downloadTimestamp = Date()
    backgroundAttributesDatabaseConnection.asyncReadWrite({ transaction in
      transaction.setObject(imageAttributes, forKey: URLString, inCollection: YapImageManagerImageAttributesCollection, withMetadata: downloadTimestamp)
    }, completionBlock: nil)
  }
  
  func updateImageAttributes(with size: CGSize, forURLString URLString: String) {
    if let imageAttributes = self.imageAttributes(forURLString: URLString) {
      if
        let width = imageAttributes["image_width"] as? NSNumber,
        let height = imageAttributes["image_height"] as? NSNumber
      {
        // update only if dimensions have changed
        let isSame = CGFloat(width.floatValue) == size.width && CGFloat(height.floatValue) == size.height
        if !isSame {
          let updatedAttributes = NSMutableDictionary(dictionary: imageAttributes)
          updatedAttributes["image_width"] = Int(size.width)
          updatedAttributes["image_height"] = Int(size.height)
          setImageAttributes(updatedAttributes, forURLString: URLString)
        }
      }
    } else {
      // no image attribute data exists, so set size and width and save to database
      let imageAttributes = ["image_width": Int(size.width), "image_height": Int(size.height)] as NSDictionary
      setImageAttributes(imageAttributes, forURLString: URLString)
    }
  }
  
  // MARK: Database cleanup

  private func vacuumDatabaseIfNeeded() {
    
    if backgroundDatabaseConnection.pragmaAutoVacuum() == "NONE" {
      // We don't vacuum right away.
      // The app just launched, so it could be pulling down stuff from the server.
      // Instead, we queue up the vacuum operation to run after a slight delay.
      DispatchQueue.main.asyncAfter(deadline: .now() + 3.0, execute: {() -> Void in
        self.backgroundDatabaseConnection.asyncVacuum {
          DDLogInfo("VACUUM complete (upgrading database auto_vacuum setting)")
        }
      })
    }
  }
  
  private func removeExpiredImages() {
    DispatchQueue.main.async(execute: {() -> Void in
      var imageDeleteKeys = [String]()
      var imageAttributeDeleteKeys = [String]()
      self.attributesDatabaseConnection.read({ transaction in
        
        transaction.enumerateKeysAndMetadata(inCollection: YapImageManagerImageAttributesCollection, using: { (key: String, metadata: Any?, stop: UnsafeMutablePointer<ObjCBool>) in
          if let created = metadata as? NSDate {
            let timeSince: TimeInterval = -created.timeIntervalSinceNow
            if timeSince > self.configuration.expireImagesAfter {
              DDLogVerbose("Remove expired image \(key)")
              imageDeleteKeys.append(key)
            }
            if timeSince > self.configuration.expireImageAttributesAfter {
              DDLogVerbose("Remove expired attributes \(key)")
              imageAttributeDeleteKeys.append(key)
            }
          }
        })
        
        // remove expired images and images, images first so you never end up with images without attributes
        self.backgroundDatabaseConnection.asyncReadWrite({ transaction  in
          // remove images
          transaction.removeObjects(forKeys: imageDeleteKeys, inCollection: YapImageManagerImageCollection)
        }, completionBlock: {() -> Void in
          // remove image attributes after removing images has completed
          self.backgroundAttributesDatabaseConnection.asyncReadWrite( { transaction in
            transaction.removeObjects(forKeys: imageAttributeDeleteKeys, inCollection: YapImageManagerImageAttributesCollection)
          }, completionBlock: {() -> Void in
            // DEBUG
            //[self validateDatabaseIntegrity];
          })
        })
        // DEBUG
        //[self validateDatabaseIntegrity];
      })
    })
  }
  
  private func validateDatabaseIntegrity() {
    var imageKeys = Set<String>()
    var imageAttributeKeys = Set<String>()
    databaseConnection.read({ transaction in
      transaction.enumerateKeysAndMetadata(inCollection: YapImageManagerImageCollection, using: { (key: String, metadata: Any?, stop: UnsafeMutablePointer<ObjCBool>) in
        imageKeys.insert(key)
      })
    })
    attributesDatabaseConnection.read({transaction in
      transaction.enumerateKeysAndMetadata(inCollection: YapImageManagerImageAttributesCollection, using: { (key: String, metadata: Any?, stop: UnsafeMutablePointer<ObjCBool>) in
        imageAttributeKeys.insert(key)
      })
    })
    // there can be more image attributes than images, in the case of cancelled downloads since we pull size info out of the image header block
    if imageKeys.isSubset(of: imageAttributeKeys) {
      DDLogInfo("- IMAGE CHECKSUM SUCCESS")
    }
    else {
      DDLogInfo("- IMAGE CHECKSUM FAILURE")
    }
  }
}

