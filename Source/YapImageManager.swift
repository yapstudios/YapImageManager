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
class ImageQueueItem: NSObject {
  var URLString: String = ""
  var size = CGSize.zero
  var progress: Progress?
  var downloadstartTime: Date? // start of download
  var options: ImageRasterizationOptions?
  
  override var description: String {
    return URLString
  }
}

public class ImageRasterizationOptions: NSObject {
  var backgroundColor: UIColor?
  var isGradient: Bool = false
  var isBevelEdge: Bool = false
  
  func key() -> String {
    var key = String()
    key += "\(isGradient ? "_G" : "")\(isBevelEdge ? "_B" : "")"
    var r = CGFloat(0.0)
    var g = CGFloat(0.0)
    var b = CGFloat(0.0)
    var a = CGFloat(0.0)
    
    if backgroundColor != nil && backgroundColor!.getRed(&r, green: &g, blue: &b, alpha: &a) {
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
  private var downloadQueue = [ImageQueueItem]()
  private var imageRequests = [ImageQueueItem]()
  private var pendingWritesDict = [AnyHashable: Any]()
  
  // image async queue
  private var useQueue2: Bool = false
  private var imageDecodeQueue1: DispatchQueue?
  private var imageDecodeQueue2: DispatchQueue?
  
  private var attributesCache: NSCache<NSString, NSDictionary>?
  private var imagesCache: NSCache<NSString, UIImage>?
  private var isReachable: Bool = false

  fileprivate static let _sharedInstance = YapImageManager()
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
  public func image(forURLString URLString: String, completion: @escaping (_ image: UIImage?, _ URLString: String) -> Void) {
    image(forURLString: URLString, size: CGSize.zero, options: nil, completion: completion)
  }
  
  /// Request a resize and decoded image async
  public func image(forURLString URLString: String, size: CGSize, completion: @escaping (_ image: UIImage?, _ URLString: String) -> Void) {
    image(forURLString: URLString, size: size, options: nil, completion: completion)
  }
  
  // Request a resized and rasterized image with rasterization options
  public func image(forURLString URLString: String, size: CGSize, options: ImageRasterizationOptions?, completion: @escaping (_ image: UIImage?, _ URLString: String) -> Void) {
    // return cached image if available
    if let cachedImage = imagesCache?.object(forKey: imageKey(forURLString: URLString, size: size, options: options) as NSString) {
      DDLogDebug("Found decoded image in memory cache \(URLString) key=\(imageKey(forURLString: URLString, size: size, options: options))")
      completion(cachedImage, URLString)
    } else {
      
      // check the pending writes cache
      var imageData = pendingWritesDict[imageKey(forURLString: URLString, size: CGSize.zero, options: nil)] as? Data
      if imageData != nil {
        DDLogDebug("Found image in pending write cache \(URLString)")
      }
      
      imageAsyncQueue().async(execute: {() -> Void in
        autoreleasepool {
          if imageData == nil {
            // check database
            self.databaseConnection.read { transaction in
              imageData = transaction.object(forKey: self.imageKey(forURLString: URLString, size: CGSize.zero, options: nil), inCollection: YapImageManagerImageCollection) as? Data
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
            var decodedImage: UIImage?
            // does image requires a resize?
            if size.equalTo(CGSize.zero) {
              // no, just decode, up to a max size
              let maxSize = self.maxSizeForImage(withSize: image.size, maxWidthHeight: CGFloat(YapImageManagerMaxDecodeWidthHeight))
              if !maxSize.equalTo(CGSize.zero) {
                DDLogVerbose("Resize image to w:\(image.size.width) h:\(image.size.height) \(URLString)")
                decodedImage = self.aspectFillImage(forImage: image, size: maxSize, options: options)
              } else {
                DDLogVerbose("Decode image with w:\(image.size.width) h:\(image.size.height) \(URLString)")
                decodedImage = self.aspectFillImage(forImage: image, size: image.size, options: options)
              }
            } else {
              // else resize
              decodedImage = self.aspectFillImage(forImage: image, size: size, options: options)
            }
            // save image in cache
            if let decodedImage = decodedImage {
              DDLogVerbose("Save decoded image to memory cache \(URLString) key=\(self.imageKey(forURLString: URLString, size: size, options: options))")
              self.imagesCache?.setObject(decodedImage, forKey: self.imageKey(forURLString: URLString, size: size, options: options) as NSString)
            }
            DispatchQueue.main.async(execute: {() -> Void in
              autoreleasepool {
                if let decodedImage = decodedImage {
                  completion(decodedImage, URLString)
                } else {
                  DDLogError("Failed to resize image with URL \(URLString)")
                }
              }
            })
          }
          else {
            DispatchQueue.main.async(execute: {() -> Void in
              autoreleasepool {
                if let image = image {
                  completion(image, URLString)
                }
                else {
                  // add to queue to download
                  self.queueImage(forURLString: URLString, size: size)
                  completion(nil, URLString)
                }
              }
            })
          }
        }
      })
    }
  }
  
  // resized image with rasterization options
  public func image(with size: CGSize, options: ImageRasterizationOptions, completion: @escaping (_ image: UIImage?) -> Void) {
    // return cached image if available
    if let cachedImage = imagesCache?.object(forKey: imageKey(forURLString: nil, size: size, options: options) as NSString) {
      DDLogVerbose("Found decoded image in memory cache key=\(imageKey(forURLString: nil, size: size, options: options))")
      completion(cachedImage)
      return
    }
    // render image
    imageAsyncQueue().async(execute: {() -> Void in
      autoreleasepool {
        if let rasterizedImage = self.aspectFillImage(forImage: nil, size: size, options: options) {
          // save image in cache
          self.imagesCache?.setObject(rasterizedImage, forKey: self.imageKey(forURLString: nil, size: size, options: options) as NSString)
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
  
  // rasterize image with background color and options
  func queueImage(forURLString URLString: String) {
    queueImage(forURLString: URLString, size: CGSize.zero)
  }
  
  func queueImage(forURLString URLString: String, size: CGSize) {
    if let foundItem = imageQueueItem(forURLString: URLString), let index = downloadQueue.index(of: foundItem) {
      // move to end of list to bump priority, if not already downloading
      downloadQueue.remove(at: index)
      downloadQueue.append(foundItem)
      DDLogVerbose("Requested image is already queued; increasing priority \(URLString)")
    } else {
      let item = ImageQueueItem()
      item.URLString = URLString
      item.size = size
      downloadQueue.append(item)
      DDLogVerbose("Queue image for download \(URLString)")
    }
    processImageQueue()
  }
  
  func isImageQueued(forURLString URLString: String) -> Bool {
    // NOTE: Active imageRequests remain on downloadQueue, so it's only necessary to check downloadQueue
    let foundItem: ImageQueueItem? = imageQueueItem(forURLString: URLString)
    return (foundItem != nil)
  }
  
  func prioritizeImage(forURLString URLString: String) {
    if let foundItem = imageQueueItem(forURLString: URLString), let index = downloadQueue.index(of: foundItem) {
      // move to end of list to bump priority, if not already downloading
      downloadQueue.remove(at: index)
      downloadQueue.append(foundItem)
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
      imageData = transaction.object(forKey: self.imageKey(forURLString: URLString, size: CGSize.zero, options: nil), inCollection: YapImageManagerImageCollection) as? Data
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
      imageData = transaction.object(forKey: self.imageKey(forURLString: URLString, size: CGSize.zero, options: nil), inCollection: YapImageManagerImageCollection) as? Data
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
  
  func aspectFillImage(forImage image: UIImage?, size: CGSize, options: ImageRasterizationOptions?) -> UIImage? {
    var aspectFillImage: UIImage?
    UIGraphicsBeginImageContextWithOptions(size, false, UIScreen.main.scale)
    // draw the image, aspect fill
    let imageRect = CGRect(x: 0, y: 0, width: size.width, height: size.height)
    guard let context = UIGraphicsGetCurrentContext() else { return nil }
    
    // draw the image aspect fill
    context.addRect(imageRect)
    context.clip()
    // Flip the context because UIKit coordinate system is upside down to Quartz coordinate system
    context.translateBy(x: 0.0, y: size.height)
    context.scaleBy(x: 1.0, y: -1.0)
    // Draw the original image to the context
    context.setBlendMode(.normal)
    // set interpolation quality
    context.interpolationQuality = .default
    if let backgroundColor = options?.backgroundColor {
      context.setFillColor(backgroundColor.cgColor)
      context.fill(imageRect)
    }
    if let image = image, let cgImage = image.cgImage {
      // Draw the image
      let aspectFillRect = aspectFillRectForImage(withSize: image.size, inRect: imageRect)
      context.draw(cgImage, in: aspectFillRect)
    }
    // Draw the gradient
    if options != nil && options!.isGradient {
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
    if options != nil && options!.isBevelEdge {
      let overlay = UIImage(named: "tileShadow")?.resizableImage(withCapInsets: UIEdgeInsetsMake(4.0, 4.0, 4.0, 4.0))
      if overlay != nil {
        overlay?.draw(in: imageRect)
      }
    }
    // capture the image
    aspectFillImage = UIGraphicsGetImageFromCurrentImageContext()
    UIGraphicsEndImageContext()
    return aspectFillImage!
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

  func downloadProgress(forURLString URLString: String) -> Progress? {
    if let imageRequest = self.imageRequest(forURLString: URLString) {
      return imageRequest.progress
    } else {
      return nil
    }
  }
  
  func saveImage(_ imageData: Data, forURLString URLString: String) {
    // save to database
    let downloadTimestamp = Date()
    pendingWritesDict[imageKey(forURLString: URLString, size: CGSize.zero, options: nil)] = imageData
    // POST notification
    let userInfo: [AnyHashable: Any] = [YapImageManagerURLKey: URLString]
    NotificationCenter.default.post(name: YapImageManagerUpdatedNotification, object: self, userInfo: userInfo)
    
    backgroundDatabaseConnection.asyncReadWrite ({ transaction in
      transaction.setObject(imageData, forKey: self.imageKey(forURLString: URLString, size: CGSize.zero, options: nil), inCollection: YapImageManagerImageCollection, withMetadata: downloadTimestamp)
    }, completionBlock: { () -> Void in
      self.pendingWritesDict.removeValue(forKey: self.imageKey(forURLString: URLString, size: CGSize.zero, options: nil))
      DDLogVerbose("Save image to database \(URLString)")
    })
  }
  
  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // MARK: - Image requests
  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // is one or more queues available
  func isReadyForImageProcessing() -> Bool {
    return ((imageRequests.count < YapImageManagerMaxSimultaneousImageRequests) && isReachable)
  }
  
  func imageRequest(forURLString URLString: String) -> ImageQueueItem? {
    let requests = imageRequests.filter { $0.URLString == URLString }
    return requests.first
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
  
  func downloadImage(forRequest imageRequest: ImageQueueItem) {
    
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
            
            self.imageAsyncQueue().async(execute: {() -> Void in
              autoreleasepool {
                // update the image attributes, if necessary
                let imageAttributes = self.imageAttributes(forURLString: imageRequest.URLString)
                var imageSize = CGSize.zero
                var shouldUpdateImageAttributes = false
                
                if let imageAttributes = imageAttributes {
                  imageSize = self.imageSize(withAttributes: imageAttributes)
                  
                } else if let image = UIImage(data: imageData!) {
                  // update image size attribute with actual image size; this should only be required if we were unable to pick up the image dimensions from the headers during download
                  let scale: CGFloat = UIScreen.main.scale
                  imageSize = CGSize(width: image.size.width * scale, height: image.size.height * scale)
                  shouldUpdateImageAttributes = true
                }
                
                // Resize image to max database size, if necessary
                let maxSize: CGSize = self.maxSizeForImage(withSize: imageSize, maxWidthHeight: CGFloat(YapImageManagerMaxDatabaseWidthHeight))
                if !maxSize.equalTo(CGSize.zero) {
                  imageSize = self.maxSizeForImage(withSize: imageSize, maxWidthHeight: CGFloat(YapImageManagerWidthHeightToResizeIfDatabaseMaxWidthHeightExceeded))
                  DDLogVerbose("Image exceeded max size for database; resizing \(imageRequest.URLString)");
                  if
                    let image = UIImage(data: imageData!),
                    let resizedImage = self.aspectFillImage(forImage: image, size: imageSize, options: nil)
                  {
                    shouldUpdateImageAttributes = true
                    imageData = UIImageJPEGRepresentation(resizedImage, 0.8)
                  }
                }
                
                // write the image attributes, if necessary
                if shouldUpdateImageAttributes {
                  DDLogVerbose("Update image attributes \(imageRequest.URLString)")
                  self.updateImageAttributes(with: imageSize, forURLString: imageRequest.URLString)
                }
                
                // dispatch next request on main thread
                DispatchQueue.main.async(execute: {() -> Void in
                  autoreleasepool {
                    // remove queue item
                    self.removeQueuedImage(forURLString: imageRequest.URLString)
                    imageRequest.progress = nil
                    self.imageRequests.remove(at: self.imageRequests.index(of: imageRequest)!)
                    if let imageData = imageData {
                      self.saveImage(imageData, forURLString: imageRequest.URLString)
                    }
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
          self.removeQueuedImage(forURLString: imageRequest.URLString)
          imageRequest.progress = nil
          self.imageRequests.remove(at: self.imageRequests.index(of: imageRequest)!)
          
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
  
  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // MARK: - Image APIs
  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  
  func imageKey(forURLString URLString: String?, size: CGSize, options: ImageRasterizationOptions?) -> String {
    var key = String()
    key += String(format: "\(URLString?.md5Hash() ?? "<na>")_%0.5f_%0.5f", size.width, size.height)
    if let options = options {
      key += options.key()
    }
    return key
  }
  
  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // MARK: - Image queue
  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  func imageQueueItem(forURLString URLString: String) -> ImageQueueItem? {
    let items = downloadQueue.filter { $0.URLString == URLString }
    return items.first
  }
  
  func removeQueuedImage(forURLString URLString: String) {
    if let foundItem = imageQueueItem(forURLString: URLString), let index = downloadQueue.index(of: foundItem) {
      downloadQueue.remove(at: index)
    }
  }
  
  func nextImageQueueItem() -> ImageQueueItem? {
    
    if let nextItem = downloadQueue.last {
      if let _ = imageRequest(forURLString: nextItem.URLString) {
        // skip
      } else {
        return nextItem
      }
    }
    return nil
  }
  
  func processImageQueue() {
    if !isReadyForImageProcessing() {
      return
    }
    // process image
    if !downloadQueue.isEmpty {
      if let item = nextImageQueueItem() {
        let imageRequest = ImageQueueItem()
        imageRequest.URLString = item.URLString
        imageRequest.size = item.size
        imageRequests.append(imageRequest)
        // start image download
        downloadImage(forRequest: imageRequest)
        DDLogVerbose("Download image \(imageRequest.URLString) (active:\(imageRequests.count) queued: \(downloadQueue.count))")
      }
    }
  }
  
  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // MARK: - Image processing queue
  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  func imageAsyncQueue() -> DispatchQueue {
    var queue: DispatchQueue? = nil
    if !useQueue2 {
      if imageDecodeQueue1 == nil {
        imageDecodeQueue1 = DispatchQueue(label: "YapImageManager.imageDecode.1")
      }
      queue = imageDecodeQueue1
    }
    else {
      if imageDecodeQueue2 == nil {
        imageDecodeQueue2 = DispatchQueue(label: "YapImageManager.imageDecode.2")
      }
      queue = imageDecodeQueue2
    }
    useQueue2 = !useQueue2
    return queue!
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
  func maxSizeForImage(withSize imageSize: CGSize, maxWidthHeight: CGFloat) -> CGSize {
    var maxSize = CGSize.zero
    if imageSize.width > maxWidthHeight || imageSize.height > maxWidthHeight {
      if imageSize.width > imageSize.height {
        maxSize = CGSize()
        maxSize.width = maxWidthHeight
        maxSize.height = (maxWidthHeight * imageSize.height / imageSize.width).rounded()
      }
      else {
        maxSize = CGSize()
        maxSize.width = (maxWidthHeight * imageSize.width / imageSize.height).rounded()
        maxSize.height = maxWidthHeight
      }
    }
    return maxSize
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

