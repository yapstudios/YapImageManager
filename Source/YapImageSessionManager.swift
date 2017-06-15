//
//  YapHTTPImageSessionManager.h
//  YapImageManager
//
//  Created by Trevor Stout on 12/9/13.
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
//
//  NOTE: 
//  This Swift file is currently being ported from the original Objective C project.
//  It is not currently included as a target in YapImageManager.

import AFNetworking
/**
 * YapHTTPImageSessionManager, a subclass of AFHTTPSessionManager, attempts to capture the image attributes for JPEG, GIF, and PNG downloads during the download stream.
 * This is useful in cases where you need the image dimentions as soon as possible to layout the UI, for example in a variable height table cell.
 **/

class YapImageSessionManager: AFHTTPSessionManager {
  weak var delegate: YapImageSessionManagerDelegate?
  var isShouldPreloadImageAttributes: Bool = false
  var isShouldPreloadGIFImageData: Bool = false
  
  private var imageAttributesCache: NSCache?
  private var imageDownloadProgressCache: NSCache?
  private var pendingGIFPreviews: NSCache?
  
  // MARK: - NSURLSessionDataTaskDelegate
  
  func downloadProgress(for task: URLSessionTask) -> Progress {
    let delegate: AFURLSessionManagerTaskDelegate? = self.delegate(for: task)
    return delegate?.downloadProgress()!
  }
  
  func urlSession(_ session: URLSession, dataTask: URLSessionDataTask, didReceiveData data: Data) {
    let isFirstPacket: Bool = (dataTask.countOfBytesReceived == data.length)
    let delegate: AFURLSessionManagerTaskDelegate? = self.delegate(for: dataTask)
    delegate?.mutableData?.append(data)
    if imageAttributesCache == nil {
      imageAttributesCache = NSCache()
      imageAttributesCache?.countLimit = 100
      imageDownloadProgressCache = NSCache()
      imageDownloadProgressCache?.countLimit = MAX_SIMULTANEOUS_IMAGE_REQUESTS
      // only need number of simultaneous requests
      pendingGIFPreviews = NSCache()
      pendingGIFPreviews?.countLimit = MAX_SIMULTANEOUS_IMAGE_REQUESTS
    }
    let key: String? = dataTask.originalRequest.url?.absoluteString
    var imageAttributes: [AnyHashable: Any]? = imageAttributesCache?.object(forKey: key)
    // if the images attributes have not been found, scan the downloaded packets
    // Only scan first 250k bytes, as image attributes are unlikely to be found after that
    if isShouldPreloadImageAttributes && !imageAttributes && (delegate?.mutableData?.length < 250000 || isFirstPacket) {
      //NSLog(@"%d bytes downloaded for URL %@", (int) delegate.mutableData.length, key);
      let context = YapHTTPImageContext()
      context.urlString = key
      context.data = delegate?.mutableData
      // clear any stale pending GIF previews
      if isFirstPacket {
        pendingGIFPreviews?.removeObject(forKey: key)
      }
      if isJPEGImage(from: context) {
        imageAttributes = jpegImageAttributes(from: context)
        if imageAttributes != nil {
          //NSLog(@"** found image attributes for %@ %@", context.URLString, imageAttributes);
          imageAttributesCache?.setObject(imageAttributes, forKey: key)
          imageAttributesFound(imageAttributes, forURLString: context.urlString)
        }
      }
      else if isGIFImage(from: context) {
        imageAttributes = gifImageAttributes(from: context)
        if imageAttributes != nil {
          //NSLog(@"** found GIF image attributes for %@ %@", context.URLString, imageAttributes);
          imageAttributesCache?.setObject(imageAttributes, forKey: key)
          imageAttributesFound(imageAttributes, forURLString: context.urlString)
          // flag this GIF to have a preview generated when the first frame is available after n bytes (est. by size * width)
          let imageSize: CGSize = YapImageManager.sharedInstance().imageSizeForImage(withAttributes: imageAttributes)
          if imageSize.width > 0.0 && imageSize.height > 0.0 {
            // make the GIF
            pendingGIFPreviews?.setObject((imageSize.width * imageSize.height), forKey: key)
          }
        }
      }
      else if isPNGImage(from: context) {
        imageAttributes = pngImageAttributes(from: context)
        if imageAttributes != nil {
          //NSLog(@"** found PNG image attributes for %@ %@", context.URLString, imageAttributes);
          imageAttributesCache?.setObject(imageAttributes, forKey: key)
          imageAttributesFound(imageAttributes, forURLString: context.urlString)
        }
      }
    }
    // check to see if first frame of GIF should be generated
    let bytesRequiredToGenerateGIFPreview = pendingGIFPreviews?.object(forKey: key)
    if isShouldPreloadGIFImageData && bytesRequiredToGenerateGIFPreview && dataTask.countOfBytesReceived >= CInt(bytesRequiredToGenerateGIFPreview) {
      pendingGIFPreviews?.removeObject(forKey: key)
      let context = YapHTTPImageContext()
      context.urlString = key
      context.data = delegate?.mutableData
      let imageData = gifImageData(from: context)
      if imageData {
        //NSLog(@"Preview for %@ generated in first %d of %d bytes", key, (int) dataTask.countOfBytesReceived, (int) dataTask.countOfBytesExpectedToReceive);
        DispatchQueue.main.async(execute: {() -> Void in
          var shouldCancelRequest: Bool = false
          if self.delegate?.responds(to: Selector("imageSessionManager:GIFImageDataFound:forURLString:shouldCancelRequest:")) {
            self.delegate?.imageSessionManager(self, gifImageDataFound: imageData, forURLString: key, shouldCancelRequest: shouldCancelRequest)
          }
          if shouldCancelRequest {
            dataTask.cancel()
          }
        })
      }
    }
    // handle download progress
    if dataTask.countOfBytesExpectedToReceive > 0 {
      let progress = CGFloat(dataTask.countOfBytesReceived) / CGFloat(dataTask.countOfBytesExpectedToReceive)
      let roundedProgress: CGFloat = roundf(progress * 10.0)
      // round to 1/10
      // cleanup cache if at beginning or end of download
      if isFirstPacket {
        imageDownloadProgressCache?.removeObject(forKey: key)
      }
      let priorProgress = imageDownloadProgressCache?.object(forKey: key)
      if priorProgress && CFloat(priorProgress) == roundedProgress {
        // skip
      }
      else {
        imageDownloadProgressCache?.setObject((roundedProgress), forKey: key)
        delegate?.downloadProgress?.totalUnitCount = Int64(10)
        delegate?.downloadProgress?.completedUnitCount = Int64(roundedProgress)
      }
    }
  }
  
  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // MARK: Notification
  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  /**
   * Handles posting notification to the main thread.
   **/
  func imageAttributesFound(_ imageAttributes: [AnyHashable: Any], forURLString URLString: String) {
    let block: () -> ()? = {() -> Void in
      if delegate?.responds(to: Selector("imageSessionManager:imageAttributesFound:forURLString:")) {
        delegate?.imageSessionManager(self, imageAttributesFound: imageAttributes, forURLString: URLString)
      }
    }
    if Thread.isMainThread {
      block()
    }
    else {
      DispatchQueue.main.async(execute: block)
    }
  }
  
  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // MARK: JPEG decoding
  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  func read_1_byte(_ context: YapHTTPImageContext, eof: Bool) -> UInt8 {
    var c: UInt8 = "\0"
    if context.dataIndex >= context.data.length {
      //NSLog(@"Premature EOF in JPEG file");
      eof = true
      return c
    }
    context.data.getBytes(c, range: NSRange(location: context.dataIndex, length: 1))
    context.dataIndex += 1
    //NSLog(@"%d %02.2hhX", context.dataIndex, c);
    eof = false
    return c
  }
  
  /* Read 2 bytes, convert to unsigned int */
  /* All 2-byte quantities in JPEG markers are MSB first */
  func read_2_bytes(_ context: YapHTTPImageContext, eof: Bool) -> UInt {
    var i: UInt
    var c1: UInt8
    var c2: UInt8
    c1 = read_1_byte(context, eof: eof)
    c2 = read_1_byte(context, eof: eof)
    i = ((UInt(c1)) << 8) + (UInt(c2))
    //NSLog(@"2 BYTES: %02.2X", i);
    return i
  }
  
  /*
   * JPEG markers consist of one or more 0xFF bytes, followed by a marker
   * code byte (which is not an FF).  Here are the marker codes of interest
   * in this program.  (See jdmarker.c for a more complete list.)
   */
  let M_SOF0 = 0xc0
  let M_SOF1 = 0xc1
  let M_SOF2 = 0xc2
  let M_SOF3 = 0xc3
  let M_SOF5 = 0xc5
  let M_SOF6 = 0xc6
  let M_SOF7 = 0xc7
  let M_SOF9 = 0xc9
  let M_SOF10 = 0xca
  let M_SOF11 = 0xcb
  let M_SOF13 = 0xcd
  let M_SOF14 = 0xce
  let M_SOF15 = 0xcf
  let M_SOI = 0xd8
  let M_EOI = 0xd9
  let M_SOS = 0xda
  let M_APP0 = 0xe0
  let M_APP12 = 0xec
  let M_COM = 0xfe
  /*
   * Find the next JPEG marker and return its marker code.
   * We expect at least one FF byte, possibly more if the compressor used FFs
   * to pad the file.
   * There could also be non-FF garbage between markers.  The treatment of such
   * garbage is unspecified; we choose to skip over it but emit a warning msg.
   * NB: this routine must not be used after seeing SOS marker, since it will
   * not deal correctly with FF/00 sequences in the compressed image data...
   */
  func next_marker(_ context: YapHTTPImageContext, eof: Bool) -> Int {
    var c: UInt8
    let discarded_bytes: Int = 0
    /* Find 0xFF byte; count and skip any non-FFs. */
    c = read_1_byte(context, eof: eof)
    while c != 0xff && !eof {
      discarded_bytes += 1
      c = read_1_byte(context, eof: eof)
    }
    /* Get marker code byte, swallowing any duplicate FF bytes.  Extra FFs
     * are legal as pad bytes, so don't count them in discarded_bytes.
     */
    repeat {
      c = read_1_byte(context, eof: eof)
    } while c == 0xff && !eof
    if discarded_bytes != 0 {
      print("Warning: garbage data found in JPEG file \(context.urlString)")
    }
    return c
  }
  
  /*
   * Read the initial marker, which should be SOI.
   * For a JFIF file, the first two bytes of the file should be literally
   * 0xFF M_SOI.  To be more general, we could use next_marker, but if the
   * input file weren't actually JPEG at all, next_marker might read the whole
   * file and then return a misleading error message...
   */
  func first_marker(_ context: YapHTTPImageContext, eof: Bool) -> Int {
    var c1: UInt8
    var c2: UInt8
    c1 = read_1_byte(context, eof: eof)
    c2 = read_1_byte(context, eof: eof)
    if c1 != 0xff || c2 != M_SOI {
      // not a JPEG file!
      eof = true
    }
    return c2
  }
  
  /*
   * Most types of marker are followed by a variable-length parameter segment.
   * This routine skips over the parameters for any marker we don't otherwise
   * want to process.
   * Note that we MUST skip the parameter segment explicitly in order not to
   * be fooled by 0xFF bytes that might appear within the parameter segment;
   * such bytes do NOT introduce new markers.
   */
  func skip_variable(_ context: YapHTTPImageContext, eof: Bool) {
    /* Skip over an unknown or uninteresting variable-length marker */
    var length: UInt
    /* Get the marker parameter length count */
    length = read_2_bytes(context, eof: eof)
    /* Length includes itself, so must be at least 2 */
    if length < 2 {
      print("Warning: erroneous JPEG marker length in \(context.urlString)")
      eof = true
      return
    }
    context.dataIndex += length - 2
    if context.dataIndex >= context.data.length {
      //NSLog(@"Premature EOF in JPEG file");
      eof = true
    }
  }
  
  func process(forMarker marker: Int) -> String {
    var process: String? = nil
    switch marker {
    case M_SOF0:
      process = "Baseline"
    case M_SOF1:
      process = "Extended sequential"
    case M_SOF2:
      process = "Progressive"
    case M_SOF3:
      process = "Lossless"
    case M_SOF5:
      process = "Differential sequential"
    case M_SOF6:
      process = "Differential progressive"
    case M_SOF7:
      process = "Differential lossless"
    case M_SOF9:
      process = "Extended sequential, arithmetic coding"
    case M_SOF10:
      process = "Progressive, arithmetic coding"
    case M_SOF11:
      process = "Lossless, arithmetic coding"
    case M_SOF13:
      process = "Differential sequential, arithmetic coding"
    case M_SOF14:
      process = "Differential progressive, arithmetic coding"
    case M_SOF15:
      process = "Differential lossless, arithmetic coding"
    default:
      process = "Unknown"
    }
    
    return process!
  }
  
  /*
   * Process a SOFn marker.
   * This code is only needed for the image dimensions...
   */
  func jpegImageAttributes(fromSOFn marker: Int, context: YapHTTPImageContext, eof: Bool) -> [AnyHashable: Any] {
    var imageAttributes: [AnyHashable: Any]? = nil
    var length: UInt
    var image_height: UInt
    var image_width: UInt
    var data_precision: UInt8
    var num_components: UInt8
    var ci: Int
    length = read_2_bytes(context, eof: eof)
    /* usual parameter length count */
    data_precision = read_1_byte(context, eof: eof)
    image_height = read_2_bytes(context, eof: eof)
    image_width = read_2_bytes(context, eof: eof)
    num_components = read_1_byte(context, eof: eof)
    let process: String = self.process(forMarker: marker)
    if length != UInt(8 + num_components * 3) {
      print("Warning: bogus SOF marker length in \(context.urlString)")
      eof = true
    }
    if eof {
      return nil
    }
    for ci in 0..<num_components {
      read_1_byte(context, eof: eof)
      /* Component ID code */
      read_1_byte(context, eof: eof)
      /* H, V sampling factors */
      read_1_byte(context, eof: eof)
      /* Quantization table number */
    }
    if !eof {
      imageAttributes = ["image_type": "jpeg", "image_height": (image_height), "image_width": (image_width), "data_precision": (data_precision), "color_components": (num_components), "process": process]
    }
    return imageAttributes!
  }
  
  /*
   * Parse the marker stream until SOS or EOI is seen;
   * display any COM markers.
   * While the companion program wrjpgcom will always insert COM markers before
   * SOFn, other implementations might not, so we scan to SOS before stopping.
   * If we were only interested in the image dimensions, we would stop at SOFn.
   * (Conversely, if we only cared about COM markers, there would be no need
   * for special code to handle SOFn; we could treat it like other markers.)
   */
  func jpegImageAttributes(from context: YapHTTPImageContext) -> [AnyHashable: Any] {
    var imageAttributes: [AnyHashable: Any]? = nil
    var marker: Int
    var eof: Bool = false
    context.dataIndex = 0
    // reset initial state of context
    /* Expect SOI at start of file */
    if first_marker(context, eof: eof) != M_SOI {
      eof = true
    }
    /* Scan miscellaneous markers until we reach SOS. */
    while !eof {
      marker = next_marker(context, eof: eof)
      switch marker {
        /* Note that marker codes 0xC4, 0xC8, 0xCC are not, and must not be,
         * treated as SOFn.  C4 in particular is actually DHT.
         */
      case M_SOF0,                 /* Baseline */
      M_SOF1,                 /* Extended sequential, Huffman */
      M_SOF2,                 /* Progressive, Huffman */
      M_SOF3,                 /* Lossless, Huffman */
      M_SOF5,                 /* Differential sequential, Huffman */
      M_SOF6,                 /* Differential progressive, Huffman */
      M_SOF7,                 /* Differential lossless, Huffman */
      M_SOF9,                 /* Extended sequential, arithmetic */
      M_SOF10,                 /* Progressive, arithmetic */
      M_SOF11,                 /* Lossless, arithmetic */
      M_SOF13,                 /* Differential sequential, arithmetic */
      M_SOF14,                 /* Differential progressive, arithmetic */
      M_SOF15:
        /* Differential lossless, arithmetic */
        imageAttributes = jpegImageAttributes(fromSOFn: marker, context: context, eof: eof)
        return imageAttributes!
      case M_SOS:
        /* stop before hitting compressed data */
        return imageAttributes!
      case M_EOI:
        /* in case it's a tables-only JPEG stream */
        return imageAttributes!
      default:
        /* Anything else just gets skipped */
        skip_variable(context, eof: eof)
        /* we assume it has a parameter count... */
        
      }
    }
    /* end loop */
    return imageAttributes!
  }
  
  /*
   * Return YES if image is JPEG
   */
  func isJPEGImage(from context: YapHTTPImageContext) -> Bool {
    var eof: Bool = false
    context.dataIndex = 0
    // reset initial state of context
    /* Expect SOI at start of file */
    if first_marker(context, eof: eof) == M_SOI {
      return true
    }
    return false
  }
  
  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // MARK: GIF decoding
  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  /*
   * Return YES if image is GIF89a or GIF87a
   */
  func isGIFImage(from context: YapHTTPImageContext) -> Bool {
    var eof: Bool = false
    context.dataIndex = 0
    // reset initial state of context
    /* Expect SOI at start of file */
    if first_GIF_marker(context, eof: eof) {
      return true
    }
    return false
  }
  
  func gifImageAttributes(from context: YapHTTPImageContext) -> [AnyHashable: Any] {
    var eof: Bool = false
    if isGIFImage(from: context) {
      let w: UInt = read_2_bytes_LSB(context, eof: eof)
      let h: UInt = read_2_bytes_LSB(context, eof: eof)
      if !eof {
        let imageAttributes: [AnyHashable: Any] = ["image_type": "gif", "image_height": (h), "image_width": (w)]
        return imageAttributes
      }
    }
    return nil
  }
  
  func gifImageData(from context: YapHTTPImageContext) -> Data {
    var eof: Bool = false
    //NSLog(@"GIF BEGIN DECODE");
    if isGIFImage(from: context) {
      skip_n_bytes(context, bytes: 4, eof: eof)
      // logical screen descriptor, 7 bytes total
      let c: UInt8 = read_1_byte(context, eof: eof)
      // packed fields
      skip_n_bytes(context, bytes: 2, eof: eof)
      let colorTableSize: Int = gifColorTableSize(forColorTable: c)
      skip_n_bytes(context, bytes: colorTableSize, eof: eof)
      // skip color table
      var marker: UInt8
      while !eof {
        marker = read_1_byte(context, eof: eof)
        context.dataIndex -= 1
        // put back marker character
        if marker == 0x3b {
          // This is the end
          //NSLog(@"GIF END DECODE");
          break
        }
        switch marker {
        case 0x21:
          // Graphic Control Extension (#n of n)
          read_1_byte(context, eof: eof)
          // skip first marker
          let `extension`: UInt8 = read_1_byte(context, eof: eof)
          context.dataIndex -= 2
          // put back extension character and marker characters
          if !eof {
            switch `extension` {
            case 0xf9:
              skipGIFExtensionGraphicControl(context, eof: eof)
            case 0xff:
              skipGIFExtensionApplication(context, eof: eof)
            case 0x01:
              skipGIFExtensionPlainText(context, eof: eof)
            case 0xfe:
              skipGIFExtensionComment(context, eof: eof)
            default:
              // error
              print("GIF ERROR READING EXTENSION MARKER")
              eof = true
            }
          }
        case 0x2c:
          // Image Descriptor (#n of n)
          skipGIFImageDescription(context, eof: eof)
          if !eof {
            // at this point we have the first image; insert EOF marker and return image data
            var imageData = Data()
            imageData.append(context.data.subdata(with: NSRange(location: 0, length: context.dataIndex)))
            var eof: UInt8 = 0x3b
            imageData.append(eof, length: 1)
            //NSLog(@"FOUND GIF FRAME FOR IMAGE %@ IN %d BYTES", context.URLString, (int) context.dataIndex);
            return imageData
          }
        default:
          // error
          print("GIF ERROR READING MARKER")
          eof = true
        }
      }
    }
    return nil
  }
  
  func skipGIFExtensionGraphicControl(_ context: YapHTTPImageContext, eof: Bool) {
    //NSLog(@"GIF SKIP GIF EXTENSION");
    skip_n_bytes(context, bytes: 8, eof: eof)
  }
  
  func skipGIFExtensionApplication(_ context: YapHTTPImageContext, eof: Bool) {
    //NSLog(@"GIF SKIP APPLICATION EXTENSION");
    skip_n_bytes(context, bytes: 14, eof: eof)
    skipGIFDataBlocks(context, eof: eof)
  }
  
  func skipGIFExtensionPlainText(_ context: YapHTTPImageContext, eof: Bool) {
    //NSLog(@"GIF SKIP PLAIN TEXT EXTENSION");
    skip_n_bytes(context, bytes: 15, eof: eof)
    skipGIFDataBlocks(context, eof: eof)
  }
  
  func skipGIFExtensionComment(_ context: YapHTTPImageContext, eof: Bool) {
    //NSLog(@"GIF SKIP COMMENT EXTENSION");
    skip_n_bytes(context, bytes: 2, eof: eof)
    skipGIFDataBlocks(context, eof: eof)
  }
  
  func gifColorTableSize(forColorTable c: UInt8) -> Int {
    // get size of global color table
    if c & 0x80 {
      let GIF_colorC: Int = (c & 0x07)
      let GIF_colorS: Int = 2 << GIF_colorC
      return (3 * GIF_colorS)
    }
    else {
      return 0
    }
  }
  
  func skipGIFImageDescription(_ context: YapHTTPImageContext, eof: Bool) {
    //NSLog(@"GIF SKIP IMAGE DESCRIPTION");
    skip_n_bytes(context, bytes: 9, eof: eof)
    let c: UInt8 = read_1_byte(context, eof: eof)
    // packed fields
    let colorTableSize: Int = gifColorTableSize(forColorTable: c)
    skip_n_bytes(context, bytes: colorTableSize, eof: eof)
    // skip color table
    skip_n_bytes(context, bytes: 1, eof: eof)
    // skip lzw
    skipGIFDataBlocks(context, eof: eof)
  }
  
  func skipGIFDataBlocks(_ context: YapHTTPImageContext, eof: Bool) {
    var totalBytes: Int = 0
    while !eof {
      let bytes: UInt8 = read_1_byte(context, eof: eof)
      if bytes == 0x00 {
        //NSLog(@"GIF SKIP DATA BLOCKS (%d)", totalBytes);
        break
      }
      totalBytes += bytes
      skip_n_bytes(context, bytes: bytes, eof: eof)
    }
  }
  
  func skip_n_bytes(_ context: YapHTTPImageContext, bytes: Int, eof: Bool) {
    context.dataIndex += bytes
    if context.dataIndex >= context.data.length {
      eof = true
    }
  }
  
  /* Read 2 bytes, convert to unsigned int */
  /* All 2-byte quantities in GIF markers are LSB first */
  func read_2_bytes_LSB(_ context: YapHTTPImageContext, eof: Bool) -> UInt {
    var i: UInt
    var c1: UInt8
    var c2: UInt8
    c1 = read_1_byte(context, eof: eof)
    c2 = read_1_byte(context, eof: eof)
    i = ((UInt(c2)) << 8) + (UInt(c1))
    //NSLog(@"2 BYTES: %02.2X", i);
    return i
  }
  
  func first_GIF_marker(_ context: YapHTTPImageContext, eof: Bool) -> Bool {
    var c1: UInt8
    var c2: UInt8
    var c3: UInt8
    var v1: UInt8
    var v2: UInt8
    var v3: UInt8
    c1 = read_1_byte(context, eof: eof)
    c2 = read_1_byte(context, eof: eof)
    c3 = read_1_byte(context, eof: eof)
    v1 = read_1_byte(context, eof: eof)
    v2 = read_1_byte(context, eof: eof)
    v3 = read_1_byte(context, eof: eof)
    if c1 == "G" && c2 == "I" && c3 == "F" {
      // not a JPEG file!
      return true
    }
    if v1 == "8" && v2 == "9" && v3 == "a" {
      // Version 89a
      return true
    }
    if v1 == "8" && v2 == "7" && v3 == "a" {
      // Version 87a... is this still used? Oh please... circa 1987
      return true
    }
    return false
  }
  
  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // MARK: PNG decoding
  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  /*
   * Return YES if image is PNG
   */
  func isPNGImage(from context: YapHTTPImageContext) -> Bool {
    var eof: Bool = false
    context.dataIndex = 0
    // reset initial state of context
    /* Expect SOI at start of file */
    if first_PNG_marker(context, eof: eof) {
      return true
    }
    return false
  }
  
  func pngImageAttributes(from context: YapHTTPImageContext) -> [AnyHashable: Any] {
    var eof: Bool = false
    if isPNGImage(from: context) {
      //		unsigned char skip;
      //		skip = [self read_1_byte:context eof:&eof]; // byte 9
      //		skip = [self read_1_byte:context eof:&eof]; // byte 10
      //		skip = [self read_1_byte:context eof:&eof]; // byte 11
      //		skip = [self read_1_byte:context eof:&eof]; // byte 12
      let c1: UInt8 = read_1_byte(context, eof: eof)
      let c2: UInt8 = read_1_byte(context, eof: eof)
      let c3: UInt8 = read_1_byte(context, eof: eof)
      let c4: UInt8 = read_1_byte(context, eof: eof)
      if !eof && c1 == "I" && c2 == "H" && c3 == "D" && c4 == "R" {
        // IHDR
        let w: UInt = read_4_bytes(context, eof: eof)
        let h: UInt = read_4_bytes(context, eof: eof)
        if !eof {
          let imageAttributes: [AnyHashable: Any] = ["image_type": "png", "image_height": (h), "image_width": (w)]
          return imageAttributes
        }
      }
    }
    return nil
  }
  
  func read_4_bytes(_ context: YapHTTPImageContext, eof: Bool) -> UInt {
    var i: UInt
    let c1: UInt8 = read_1_byte(context, eof: eof)
    let c2: UInt8 = read_1_byte(context, eof: eof)
    let c3: UInt8 = read_1_byte(context, eof: eof)
    let c4: UInt8 = read_1_byte(context, eof: eof)
    i = ((UInt(c1)) << 24) + ((UInt(c2)) << 16) + ((UInt(c3)) << 8) + (UInt(c4))
    return i
  }
  
  func first_PNG_marker(_ context: YapHTTPImageContext, eof: Bool) -> Bool {
    let c1: UInt8 = read_1_byte(context, eof: eof)
    let c2: UInt8 = read_1_byte(context, eof: eof)
    let c3: UInt8 = read_1_byte(context, eof: eof)
    let c4: UInt8 = read_1_byte(context, eof: eof)
    let c5: UInt8 = read_1_byte(context, eof: eof)
    let c6: UInt8 = read_1_byte(context, eof: eof)
    let c7: UInt8 = read_1_byte(context, eof: eof)
    let c8: UInt8 = read_1_byte(context, eof: eof)
    if c1 == 0x89 && c2 == 0x50 && c3 == 0x4e && c4 == 0x47 && c5 == 0x0d && c6 == 0x0a && c7 == 0x1a && c8 == 0x0a {
      // PNG file!
      return true
    }
    return false
  }
}

protocol YapImageSessionManagerDelegate: NSObjectProtocol {
  // returns the image attributes, if available, decoded in realtime from the download stream
  func imageSessionManager(_ sessionManager: YapImageSessionManager, imageAttributesFound imageAttributes: [AnyHashable: Any], forURLString URLString: String)
  
  // returns the first frame of a GIF image, if available, decoded in realtime from the download stream
  func imageSessionManager(_ sessionManager: YapImageSessionManager, gifImageDataFound imageData: Data, forURLString URLString: String, shouldCancelRequest: Bool)
}

extension YapImageSessionManagerDelegate {
  // optional
  func imageSessionManager(_ sessionManager: YapImageSessionManager, gifImageDataFound imageData: Data, forURLString URLString: String, shouldCancelRequest: Bool) {}
}

//
//  YapHTTPImageSessionManager.m
//  Feedworthy
//
//  Created by Trevor Stout on 12/9/13.
//  Copyright (c) 2013 Yap Studios LLC. All rights reserved.
//

class YapHTTPImageContext: NSObject {
  var urlString: String = ""
  var data: Data?
  var dataIndex: Int = 0
}

// TODO: currently accessing private variables; move to more complex subclassing
class AFURLSessionManagerTaskDelegate: NSObject {
  var mutableData: Data?
  var downloadProgress: Progress?
}

extension YapImageSessionManager {
  func delegate(for task: URLSessionTask) -> AFURLSessionManagerTaskDelegate {
  }
}
