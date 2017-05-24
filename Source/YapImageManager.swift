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

public let YapImageManagerExpireImagesAfter = 172800.0 // 48 hours
public let YapImageManagerExpireImageAttributesAfter = 1209600.0 // 2 weeks
public let YapImageManagerUpdatedNotification = Notification.Name(rawValue: "YapImageManagerUpdatedNotification")
public let YapImageManagerFailedNotification = Notification.Name(rawValue: "YapImageManagerFailedNotification")
public let YapImageManagerImageWillBeginDownloadingNotification = Notification.Name(rawValue: "YapImageManagerImageWillBeginDownloadingNotification")
public let YapImageManagerImageAttributesUpdatedNotification = Notification.Name(rawValue: "YapImageManagerImageAttributesUpdatedNotification")
public let YapImageManagerURLKey: String = "image_url"
public let YapImageManagerImageAttributesKey: String = "image_attributes"
public let YapImageManagerImageCollection: String = "images"
public let YapImageManagerImageAttributesCollection: String = "image_attributes"
public let YapImageManagerMaxDecodeWidthHeight = 1024.0
public let YapImageManagerMaxDatabaseWidthHeight = 4096.0
public let YapImageManagerWidthHeightToResizeIfDatabaseMaxWidthHeightExceeded = 1024.0
public let YapImageManagerMaxSimultaneousImageRequests = 5
public let YapImageManagerAcceptableContentTypes = ["image/tiff", "image/jpeg", "image/jpg", "image/gif", "image/png", "image/ico", "image/x-icon", "image/bmp", "image/x-bmp", "image/x-xbitmap", "image/x-win-bitmap"]

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// MARK: - ImageQueueItem
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

public typealias ImageRequestCompletion = (_ result: ImageResult) -> Void
public typealias ImageRequestTicket = String

public enum YapImageManagerError: Error {
  
  /// Unable to decode image
  case imageDecodeFailure
}

public class ImageResult {
  
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

class ImageRequestResponseType {
  
  var ticket: ImageRequestTicket
  var completion: ImageRequestCompletion
  var size: CGSize?
  var options: ImageRasterizationOptions?
  
  init(ticket: ImageRequestTicket, completion: @escaping ImageRequestCompletion, size: CGSize? = nil, options: ImageRasterizationOptions? = nil) {
    self.ticket = ticket
    self.completion = completion
    self.size = size
    self.options = options
  }
}

class ImageRequest: NSObject {
  var URLString: String = ""
  var progress: Progress?
  var downloadstartTime: Date? // start of download
  var responses = [ImageRequestResponseType]()
  
  override var description: String {
    return URLString
  }
}

public class ImageRasterizationOptions: NSObject {
  public var renderBackgroundColor: UIColor?
  public var renderGradient: Bool = false
  public var renderOverlayImage: Bool = false
  
  func key() -> String {
    var key = String()
    key += "\(renderGradient ? "_G" : "")\(renderOverlayImage ? "_B" : "")"
    var r = CGFloat(0.0)
    var g = CGFloat(0.0)
    var b = CGFloat(0.0)
    var a = CGFloat(0.0)
    
    if renderBackgroundColor != nil && renderBackgroundColor!.getRed(&r, green: &g, blue: &b, alpha: &a) {
      key += String(format: "_C%0.5f%0.5f%0.5f%0.5f", r, g, b, a)
    }
    return key
  }
}

public class YapImageManager: NSObject {

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
  
  /// Set to draw
  public var overlayImage: UIImage?
  
  public static func sharedInstance() -> YapImageManager {
    return _sharedInstance
  }
	
  override init() {
		
		func databasePath() -> String {
      let paths: [Any] = NSSearchPathForDirectoriesInDomains(.cachesDirectory, .userDomainMask, true)
      let databaseDir: String? = (paths.count > 0) ? (paths[0] as? String) : NSTemporaryDirectory()
      let databaseName: String = "YapImageManager.sqlite"
      return URL(fileURLWithPath: databaseDir!).appendingPathComponent(databaseName).absoluteString
    }
    
    func attributesDatabasePath() -> String {
      let paths: [Any] = NSSearchPathForDirectoriesInDomains(.cachesDirectory, .userDomainMask, true)
      let databaseDir: String? = (paths.count > 0) ? (paths[0] as? String) : NSTemporaryDirectory()
      let databaseName: String = "YapURLImageAttributes.sqlite"
      return URL(fileURLWithPath: databaseDir!).appendingPathComponent(databaseName).absoluteString
    }

    // Create a session configuration with no cache
    let config = URLSessionConfiguration.ephemeral
    // Note: this is not the same a max concurrent operations
    config.httpMaximumConnectionsPerHost = YapImageManagerMaxSimultaneousImageRequests
    config.timeoutIntervalForRequest = 30.0 // 30 second timeout
    sessionManager = SessionManager(configuration: config)

    // Initialize database and connections
    let options = YapDatabaseOptions()
    options.pragmaPageSize = 32768
    options.aggressiveWALTruncationSize = 1024 * 1024 * 100
    database = YapDatabase(path: databasePath(), objectSerializer: nil, objectDeserializer: nil, metadataSerializer: nil, metadataDeserializer: nil, objectPreSanitizer: nil, objectPostSanitizer: nil, metadataPreSanitizer: nil, metadataPostSanitizer: nil, options: options)
    
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
    
    attributesDatabase = YapDatabase(path: attributesDatabasePath())
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

    super.init()

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
  
  deinit {
    NotificationCenter.default.removeObserver(self)
  }

  /// Request a full size, decoded image async
  public func asyncImage(forURLString URLString: String, completion: @escaping ImageRequestCompletion) -> ImageRequestTicket {
    return asyncImage(forURLString: URLString, size: nil, options: nil, completion: completion)
  }
  
  /// Request a resize and decoded image async
  public func asyncImage(forURLString URLString: String, size: CGSize?, completion: @escaping ImageRequestCompletion) -> ImageRequestTicket {
    return asyncImage(forURLString: URLString, size: size, options: nil, completion: completion)
  }
  
  /// Request a resized and rasterized image with rasterization options
  public func asyncImage(forURLString URLString: String, size: CGSize?, options: ImageRasterizationOptions?, completion: @escaping ImageRequestCompletion) -> ImageRequestTicket {
		let ticket: ImageRequestTicket = UUID().uuidString
		let key = self.keyForImage(withURLString: URLString, size: size, options: options)
		
		// return cached image if available
    if let cachedImage = imagesCache?.object(forKey: key as NSString) {
      DDLogDebug("Found decoded image in memory cache \(URLString) key=\(key)")
			DispatchQueue.main.async(execute: {() -> Void in
				autoreleasepool {
					completion(ImageResult(URLString: URLString, ticket: ticket, image: cachedImage))
				}
			})
    } else {
      
      // check the pending writes cache
      var imageData = pendingWritesDict[keyForImage(withURLString: URLString, size: CGSize.zero, options: nil)] as? Data
      if imageData != nil {
        DDLogDebug("Found image in pending write cache \(URLString)")
      }
      
      imageDecodeQueue.async(execute: {() -> Void in
        autoreleasepool {
          if imageData == nil {
            // check database
            self.databaseConnection.read { transaction in
              imageData = transaction.object(forKey: self.keyForImage(withURLString: URLString, size: CGSize.zero, options: nil), inCollection: YapImageManagerImageCollection) as? Data
              if imageData != nil {
                DDLogVerbose("Found image in database \(URLString)")
              }
            }
          }
          // TODO: check to see if image is old and redownload on background queue
          var image: UIImage?
          if let imageData = imageData {
            image = UIImage(data: imageData, scale: UIScreen.main.scale)
          }
          // if the full sized image was found, save resized thumbnail size to disk and return
          if let image = image {
						
						let decodedImage = self.decodedImage(forImage: image, resizedTo: size, options: options)

            if let decodedImage = decodedImage {
							// save image in cache
              DDLogVerbose("Save decoded image to memory cache \(URLString) key=\(key)")
              self.imagesCache?.setObject(decodedImage, forKey: key as NSString)
            }
						
            DispatchQueue.main.async(execute: {() -> Void in
              autoreleasepool {
                if let decodedImage = decodedImage {
									completion(ImageResult(URLString: URLString, ticket: ticket, image: decodedImage))
                } else {
                  DDLogError("Failed to decode image with URL \(URLString)")
                }
              }
            })
          } else {
            DispatchQueue.main.async(execute: {() -> Void in
              autoreleasepool {
                if let image = image {
									completion(ImageResult(URLString: URLString, ticket: ticket, image: image))
                }
                else {
                  // add to queue to download
									let response = ImageRequestResponseType(
										ticket: ticket,
										completion: completion,
										size: size,
										options: options
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
	
  // resized image with rasterization options
  public func image(with size: CGSize, options: ImageRasterizationOptions, completion: @escaping (_ image: UIImage?) -> Void) {
    // return cached image if available
    if let cachedImage = imagesCache?.object(forKey: keyForImage(withURLString: nil, size: size, options: options) as NSString) {
      DDLogVerbose("Found decoded image in memory cache key=\(keyForImage(withURLString: nil, size: size, options: options))")
      completion(cachedImage)
      return
    }
    // render image
    imageDecodeQueue.async(execute: {() -> Void in
      autoreleasepool {
        if let rasterizedImage = self.aspectFillImage(forImage: nil, size: size, options: options) {
          // save image in cache
          self.imagesCache?.setObject(rasterizedImage, forKey: self.keyForImage(withURLString: nil, size: size, options: options) as NSString)
          DispatchQueue.main.async(execute: {() -> Void in
            autoreleasepool {
              completion(rasterizedImage)
            }
          })
        } else {
          DDLogError("Failed to rasterize image")
          completion(nil)
        }
      }
    })
  }
  
  func queueImage(forURLString URLString: String, response: ImageRequestResponseType) {
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
  
  func prioritizeImage(forURLString URLString: String) {
    if let foundItem = queuedImageRequest(forURLString: URLString), let index = queuedRequests.index(of: foundItem) {
      // move to end of list to bump priority, if not already downloading
      queuedRequests.remove(at: index)
      queuedRequests.append(foundItem)
      DDLogVerbose("Increasing priority for download \(URLString)")
    }
  }
  
  // these methods are synchronous, so not recommened for use during scrolling
  func cachedImage(forURLString URLString: String) -> UIImage? {
    return cachedImage(forURLString: URLString, size: CGSize.zero)
  }
  
  // full sized image
  func cachedImage(forURLString URLString: String, size: CGSize) -> UIImage? {
    // check the database
    var imageData: Data? = nil
    databaseConnection.read { transaction in
      imageData = transaction.object(forKey: self.keyForImage(withURLString: URLString, size: CGSize.zero, options: nil), inCollection: YapImageManagerImageCollection) as? Data
    }
    if let imageData = imageData {
      return UIImage(data: imageData, scale: UIScreen.main.scale)
    }
    return nil
  }
  
  // resized image
  func imageData(forURLString URLString: String) -> Data? {
    // check the database
    var imageData: Data? = nil
    databaseConnection.read { transaction in
      imageData = transaction.object(forKey: self.keyForImage(withURLString: URLString, size: CGSize.zero, options: nil), inCollection: YapImageManagerImageCollection) as? Data
    }
    return imageData
  }
  
  // image attributes
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
  
  func downloadProgress(forURLString URLString: String) -> Progress? {
    if let imageRequest = self.activeImageRequest(forURLString: URLString) {
      return imageRequest.progress
    } else {
      return nil
    }
  }
  
  func saveImage(_ imageData: Data, forURLString URLString: String) {
    // save to database
    let downloadTimestamp = Date()
    pendingWritesDict[keyForImage(withURLString: URLString, size: CGSize.zero, options: nil)] = imageData
		
    backgroundDatabaseConnection.asyncReadWrite ({ transaction in
      transaction.setObject(imageData, forKey: self.keyForImage(withURLString: URLString, size: CGSize.zero, options: nil), inCollection: YapImageManagerImageCollection, withMetadata: downloadTimestamp)
    }, completionBlock: { () -> Void in
      self.pendingWritesDict.removeValue(forKey: self.keyForImage(withURLString: URLString, size: CGSize.zero, options: nil))
      DDLogVerbose("Save image to database \(URLString)")
    })
  }
  
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
  
  func downloadImage(forRequest imageRequest: ImageRequest) {
    
    let request = sessionManager.request(imageRequest.URLString)
      .validate(contentType: YapImageManagerAcceptableContentTypes)
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
								if let _ = self.maxSizeForImage(withSize: imageSize, maxWidthHeight: CGFloat(YapImageManagerMaxDatabaseWidthHeight)) {
                  imageSize = self.maxSizeForImage(withSize: imageSize, maxWidthHeight: CGFloat(YapImageManagerWidthHeightToResizeIfDatabaseMaxWidthHeightExceeded)) ?? imageSize
                  DDLogVerbose("Image exceeded max size for database; resizing \(imageRequest.URLString)");
                  if let resizedImage = self.aspectFillImage(forImage: image, size: imageSize, options: nil) {
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
										let decodedImage = self.decodedImage(forImage: image, resizedTo: completion.size, options: completion.options)
										
										if let decodedImage = decodedImage {
											// save image in cache
											let key = self.keyForImage(withURLString: imageRequest.URLString, size: completion.size, options: completion.options)
											DDLogVerbose("Save decoded image to memory cache \(imageRequest.URLString) key=\(key)")
											self.imagesCache?.setObject(decodedImage, forKey: key as NSString)
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
                      self.saveImage(imageData, forURLString: imageRequest.URLString)
                    }
										
										// call completion blocks
										for imageResponse in imageRequest.responses {
											
											let key = self.keyForImage(withURLString: imageRequest.URLString, size: imageResponse.size, options: imageResponse.options)
											if let cachedImage = self.imagesCache?.object(forKey: key as NSString) {
												DDLogDebug("Return decoded image after download \(imageRequest.URLString) key=\(key)")
												imageResponse.completion(ImageResult(URLString: imageRequest.URLString, ticket: imageResponse.ticket, image: cachedImage))
											} else {
												// An error occurred
												imageResponse.completion(ImageResult(URLString: imageRequest.URLString, ticket: imageResponse.ticket, image: nil, error: YapImageManagerError.imageDecodeFailure))
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
						imageResponse.completion(ImageResult(URLString: imageRequest.URLString, ticket: imageResponse.ticket, image: nil, error: error))
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
  
  func keyForImage(withURLString URLString: String?, size: CGSize?, options: ImageRasterizationOptions?) -> String {
    var key = String()
    key += String(format: "\(URLString?.md5Hash() ?? "<na>")_%0.5f_%0.5f", size?.width ?? 0.0, size?.height ?? 0.0)
    if let options = options {
      key += options.key()
    }
    return key
  }
  
  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // MARK: - Image queue
  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
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
	

	// is one or more queues available
	func isReadyForRequest() -> Bool {
		return ((activeRequests.count < YapImageManagerMaxSimultaneousImageRequests) && isReachable)
	}
	
  func processImageQueue() {
    if !isReadyForRequest() {
      return
    }
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
  
  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // MARK: - Image attributes
  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  /**
   * Handles posting notification to the main thread.
   **/
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
  
  // return CGRectZero if image does not need resizing, otherwise a new size with same aspect
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

	func decodedImage(forImage image: UIImage, resizedTo size: CGSize? = nil, options: ImageRasterizationOptions? = nil) -> UIImage? {
		
		var decodedImage: UIImage?

		if let size = size {
			// resize image to size specified
			decodedImage = self.aspectFillImage(forImage: image, size: size, options: options)
		}	else {
			// otherwise, decode up to a max size
			if let maxSize = self.maxSizeForImage(withSize: image.size, maxWidthHeight: CGFloat(YapImageManagerMaxDecodeWidthHeight)) {
				decodedImage = self.aspectFillImage(forImage: image, size: maxSize, options: options)
			} else {
				decodedImage = self.aspectFillImage(forImage: image, size: image.size, options: options)
			}
		}
		return decodedImage
	}
	
	func aspectFillImage(forImage image: UIImage?, size: CGSize, options: ImageRasterizationOptions?) -> UIImage? {
		var aspectFillImage: UIImage?
		UIGraphicsBeginImageContextWithOptions(size, false, UIScreen.main.scale)
		let imageRect = CGRect(x: 0, y: 0, width: size.width, height: size.height)
		guard let context = UIGraphicsGetCurrentContext() else { return nil }
		
		// Add a clip rect for aspect fill
		context.addRect(imageRect)
		context.clip()
		
		// Flip the context because UIKit coordinate system is upside down to Quartz coordinate system
		context.translateBy(x: 0.0, y: size.height)
		context.scaleBy(x: 1.0, y: -1.0)
		context.setBlendMode(.normal)
		context.interpolationQuality = .default
		
		// Render the background color, if necessary
		if let renderBackgroundColor = options?.renderBackgroundColor {
			context.setFillColor(renderBackgroundColor.cgColor)
			context.fill(imageRect)
		}
		
		// Draw the image
		if let image = image, let cgImage = image.cgImage {
			let aspectFillRect = aspectFillRectForImage(withSize: image.size, inRect: imageRect)
			context.draw(cgImage, in: aspectFillRect)
		}
		
		// Draw the gradient, if necessary
		if options != nil && options!.renderGradient {
			let num_locations: size_t = 2
			let locations: [CGFloat] = [0.0, 1.0]
			let components: [CGFloat] = [
				0.0, 0.0, 0.0, 0.5, // Start color
				0.0, 0.0, 0.0, 0.0 // End color
			]
			
			if let gradient = CGGradient(colorSpace: CGColorSpaceCreateDeviceRGB(), colorComponents: components, locations: locations, count: num_locations)
			{
				let top = CGPoint(x: CGFloat(0.0), y: CGFloat(0.0))
				let bottom = CGPoint(x: CGFloat(0.0), y: CGFloat(size.height))
				context.drawLinearGradient(gradient, start: top, end: bottom, options: [])
			}
		}
		
		// Render the overlay image, if necessary
		if options != nil && options!.renderOverlayImage, let overlayImage = overlayImage {
			overlayImage.draw(in: imageRect)
		}
		
		// capture the image
		aspectFillImage = UIGraphicsGetImageFromCurrentImageContext()
		UIGraphicsEndImageContext()
		return aspectFillImage
	}
	
	func aspectFillRectForImage(withSize size: CGSize, inRect rect: CGRect) -> CGRect {
		let targetAspectRatio: CGFloat = rect.size.width / rect.size.height
		let imageAspectRatio: CGFloat = size.width / size.height
		var newRect: CGRect = rect
		// if approx the same, return target rect
		if fabs(targetAspectRatio - imageAspectRatio) < 0.00000001 {
			// close enough!
			let dx: CGFloat = size.width - rect.size.width
			let dy: CGFloat = size.height - rect.size.height
			if dx > 0 && dx < 5 && dy > 0 && dy < 5 {
				// if the image is relatively close to target, don't resize to avoid blurry images
				newRect = rect.insetBy(dx: CGFloat(-dx / 2.0), dy: CGFloat(-dy / 2.0)).integral
			}
		}
		else if imageAspectRatio > targetAspectRatio {
			// image is too wide, fix width, crop left/right
			newRect.size.width = (rect.size.height * imageAspectRatio).rounded()
			newRect.origin.x -= ((newRect.size.width - rect.size.width) / 2.0).rounded()
		}
		else if imageAspectRatio < targetAspectRatio {
			// image is too tall, fix height, crop top/bottom
			newRect.size.height = (rect.size.width / imageAspectRatio).rounded()
			newRect.origin.y -= ((newRect.size.height - rect.size.height) / 2.0).rounded()
		}
		return newRect
	}
	
  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // MARK: - Image Session Manager
  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//  func imageSessionManager(_ sessionManager: YapImageSessionManager, imageAttributesFound imageAttributes: [AnyHashable: Any], forURLString URLString: String) {
//    // save to database
//    // TODO: enable image attributes
//    //setImageAttributes(imageAttributes, forURLString: URLString)
//  }
  
  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // MARK: - Cleanup
  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  func removeExpiredImages() {
    DispatchQueue.main.async(execute: {() -> Void in
      autoreleasepool {
        var imageDeleteKeys = [String]()
        var imageAttributeDeleteKeys = [String]()
        self.attributesDatabaseConnection.read({ transaction in
          
          transaction.enumerateKeysAndMetadata(inCollection: YapImageManagerImageAttributesCollection, using: { (key: String, metadata: Any?, stop: UnsafeMutablePointer<ObjCBool>) in
            if let created = metadata as? NSDate {
              let timeSince: TimeInterval = -created.timeIntervalSinceNow
              if timeSince > Double(YapImageManagerExpireImagesAfter) {
                DDLogVerbose("Remove expired image \(key)")
                imageDeleteKeys.append(key)
              }
              if timeSince > Double(YapImageManagerExpireImageAttributesAfter) {
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

