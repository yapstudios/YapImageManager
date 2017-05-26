//
//  YapImageManager.swift
//  Feedworthy
//
//  Created by Trevor Stout on 11/8/13.
//  Copyright (c) 2013 Yap Studios LLC. All rights reserved.
//

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
  
  var databaseName: String
  var attributesDatabaseName: String
  var expireImagesAfter: TimeInterval
  var expireImageAttributesAfter: TimeInterval
  var maxDecodeWidthHeight: Float
  var maxSaveToDatabaseWidthHeight: Float
  var widthToResizeIfDatabaseMaxWidthHeightExceeded: Float
  var maxSimultaneousImageRequests: Int
  var timeoutIntervalForRequest: TimeInterval
  var acceptableContentTypes: [String]
}

public typealias ImageRequestTicket = String

public typealias ImageRequestCompletion = (_ result: ImageResponse) -> Void

public class ImageResponse {
  
  public var URLString: String
  public var ticket: ImageRequestTicket
  public var image: UIImage?
  public var error: Error?
  
  init(URLString: String, ticket: ImageRequestTicket, image: UIImage?, error: Error? = nil) {
    self.URLString = URLString
    self.ticket = ticket
    self.image = image
    self.error = error
  }
}

class ImageResponseHandler {
  
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

class ImageRequest: NSObject {
  
  var URLString: String = ""
  var progress: Progress?
  var downloadstartTime: Date? // start of download
  var responses = [ImageResponseHandler]()
  
  override var description: String {
    return URLString
  }
}

public enum YapImageManagerError: Error {
  
  /// Unable to decode image
  case imageDecodeFailure
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

  fileprivate static let _sharedInstance = YapImageManager()
  
  public static func sharedInstance() -> YapImageManager {
    return _sharedInstance
  }

  // MARK: Initialization

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
  
  init(configuration: YapImageManagerConfiguration = YapImageManager.defaultConfiguration()) {
		
    func databasePath(withName name: String) -> String {
      let paths: [Any] = NSSearchPathForDirectoriesInDomains(.cachesDirectory, .userDomainMask, true)
      let databaseDir: String? = (paths.count > 0) ? (paths[0] as? String) : NSTemporaryDirectory()
      return URL(fileURLWithPath: databaseDir!).appendingPathComponent(name).absoluteString
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

  // MARK: Public image request methods

  /// Request an image
  @discardableResult
  public func asyncImage(forURLString URLString: String, size: CGSize? = nil, filters: [YapImageFilter]? = [AspectFillFilter()], completion: @escaping ImageRequestCompletion) -> ImageRequestTicket {
		let ticket: ImageRequestTicket = UUID().uuidString
		let key = self.keyForImage(withURLString: URLString, size: size, filters: filters)
		
		// return cached image if available
    if let cachedImage = imagesCache?.object(forKey: key as NSString) {
      DDLogDebug("Found decoded image in memory cache \(URLString) key=\(key)")
			DispatchQueue.main.async(execute: {() -> Void in
				autoreleasepool {
					completion(ImageResponse(URLString: URLString, ticket: ticket, image: cachedImage))
				}
			})
    } else {
      
      // check the pending writes cache
      var imageData = pendingWritesDict[keyForImage(withURLString: URLString)] as? Data
      if imageData != nil {
        DDLogDebug("Found image in pending write cache \(URLString)")
      }
      
      imageDecodeQueue.async(execute: {() -> Void in
        autoreleasepool {
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
          // if the full sized image was found, save resized thumbnail size to disk and return
          if let image = image {
						
						let filteredImage = self.filteredImage(forImage: image, resizedTo: size, filters: filters)

            if let filteredImage = filteredImage {
							// save image in cache
              DDLogVerbose("Save filtered image to memory cache \(URLString) key=\(key)")
              self.imagesCache?.setObject(filteredImage, forKey: key as NSString)
            }
						
            DispatchQueue.main.async(execute: {() -> Void in
              autoreleasepool {
                if let filteredImage = filteredImage {
									completion(ImageResponse(URLString: URLString, ticket: ticket, image: filteredImage))
                } else {
                  DDLogError("Failed to filter image with URL \(URLString)")
                }
              }
            })
          } else {
            DispatchQueue.main.async(execute: {() -> Void in
              autoreleasepool {
                if let image = image {
									completion(ImageResponse(URLString: URLString, ticket: ticket, image: image))
                }
                else {
                  // add to queue to download
									let response = ImageResponseHandler(
										ticket: ticket,
										completion: completion,
										size: size,
										filters: filters
									)
                  self.queueImage(forURLString: URLString, response: response)
                }
              }
            })
          }
        }
      })
    }
		return ticket
  }
	
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
	
  /// Create a new image using image filters
  public func createImage(with size: CGSize, filters: [YapImageFilter], completion: @escaping (_ image: UIImage?) -> Void) {
    
    let key = keyForImage(withURLString: nil, size: size, filters: filters)
    
    // Return cached image if available
    if let cachedImage = imagesCache?.object(forKey: key as NSString) {
      DDLogVerbose("Found filtered image in memory cache key=\(key)")
      completion(cachedImage)
      return
    }
    // Render image using filters
    imageDecodeQueue.async(execute: {() -> Void in
      autoreleasepool {
        if let filteredImage = self.filteredImage(forImage: nil, size: size, filters: filters) {
          // save image in cache
          self.imagesCache?.setObject(filteredImage, forKey: key as NSString)
          DispatchQueue.main.async(execute: {() -> Void in
            autoreleasepool {
              completion(filteredImage)
            }
          })
        } else {
          DDLogError("Failed to create image")
          completion(nil)
        }
      }
    })
  }

  /// Increase the priority of an image request
  public func prioritizeImage(forURLString URLString: String) {

    if let foundItem = queuedImageRequest(forURLString: URLString), let index = queuedRequests.index(of: foundItem) {
      // move to end of list to bump priority, if not already downloading
      queuedRequests.remove(at: index)
      queuedRequests.append(foundItem)
      DDLogVerbose("Increasing priority for download \(URLString)")
    }
  }
  
  /// Returns a sized and filtered image from the image cache, if available
  public func cachedImage(forURLString URLString: String, size: CGSize? = nil, filters: [YapImageFilter]? = [AspectFillFilter()]) -> UIImage? {

    let key = keyForImage(withURLString: URLString, size: size, filters: filters)
    
    if let cachedImage = imagesCache?.object(forKey: key as NSString) {
      return cachedImage
    }
    return nil
  }

  /// Returns full sized, original image directly from the database if available, otherwise nil
  /// This method is synchronous, so not recommened for use during scrolling
  public func databaseImage(forURLString URLString: String) -> UIImage? {
    if let imageData = databaseImageData(forURLString: URLString) {
      return UIImage(data: imageData, scale: UIScreen.main.scale)
    }
    return nil
  }
  
  /// Returns full sized, original image data directly from the database if available, otherwise nil
  /// This method is synchronous, so not recommened for use during scrolling
  public func databaseImageData(forURLString URLString: String) -> Data? {
    // check the database
    var imageData: Data? = nil
    databaseConnection.read { transaction in
      imageData = transaction.object(forKey: self.keyForImage(withURLString: URLString), inCollection: YapImageManagerImageCollection) as? Data
    }
    return imageData
  }

  /// Returns the Progress for an active download request, or nil if not available
  /// Progress is available after YapImageManagerImageWillBeginDownloadingNotification
  public func downloadProgress(forURLString URLString: String) -> Progress? {
    if let imageRequest = self.activeImageRequest(forURLString: URLString) {
      return imageRequest.progress
    } else {
      return nil
    }
  }
  
  // MARK: Image queue

  func queueImage(forURLString URLString: String, response: ImageResponseHandler) {
    
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
  
  func isImageQueued(forURLString URLString: String) -> Bool {
    
    // NOTE: Active imageRequests remain on downloadQueue, so it's only necessary to check downloadQueue
    let foundItem: ImageRequest? = queuedImageRequest(forURLString: URLString)
    return (foundItem != nil)
  }
  
  func queuedImageRequest(forURLString URLString: String) -> ImageRequest? {
    
    let items = queuedRequests.filter { $0.URLString == URLString }
    return items.first
  }
  
  func removeQueuedImageRequest(forURLString URLString: String) {
    
    if let foundItem = queuedImageRequest(forURLString: URLString), let index = queuedRequests.index(of: foundItem) {
      queuedRequests.remove(at: index)
    }
  }
  
  func nextImageRequest() -> ImageRequest? {
    
    if let nextRequest = queuedRequests.last {
      if let _ = activeImageRequest(forURLString: nextRequest.URLString) {
        // skip
      } else {
        return nextRequest
      }
    }
    return nil
  }
	
	func activeImageRequest(forURLString URLString: String) -> ImageRequest? {
    
		let requests = activeRequests.filter { $0.URLString == URLString }
		return requests.first
	}
	
	// Returns true if active requests have not exceeded max simultaneous requests
	func isReadyForRequest() -> Bool {
		return ((activeRequests.count < configuration.maxSimultaneousImageRequests) && isReachable)
	}
	
  func processImageQueue() {
    
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

  func downloadImage(forRequest imageRequest: ImageRequest) {
    
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
              autoreleasepool {
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
                  autoreleasepool {
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
                  }
                })
              }
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
  
  func saveImageToDatabase(_ imageData: Data, forURLString URLString: String) {
    
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
    key += String(format: "\(URLString?.md5Hash() ?? "<na>")_%0.5f_%0.5f", size?.width ?? 0.0, size?.height ?? 0.0)
    
    if let filters = filters {
      for filter in filters {
        key += filter.key
      }
    }
    return key
  }
  
  // MARK: Image filters
  
  func filteredImage(forImage image: UIImage, resizedTo size: CGSize? = nil, filters: [YapImageFilter]? = nil) -> UIImage? {
    
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
  
  func filteredImage(forImage image: UIImage?, size: CGSize, filters: [YapImageFilter]? = nil) -> UIImage? {
    
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
      let filter = AspectFillFilter()
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

  func vacuumDatabaseIfNeeded() {
    
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
  
  func removeExpiredImages() {
    DispatchQueue.main.async(execute: {() -> Void in
      autoreleasepool {
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
      }
    })
  }
  
  func validateDatabaseIntegrity() {
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

