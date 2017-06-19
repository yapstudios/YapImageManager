//
//  YapImageFilter.swift
//  YapImageManager
//
//  Created by Trevor Stout on 5/24/17.
//  Copyright (c) 2017 Yap Studios LLC. All rights reserved.
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

import Foundation

// MARK: YapImageFilter protocol

/// A protocol used by YapImageManager to render images in one pass using the same bitmap context used to decode the
/// downloaded image. Implement the draw() method and the ‘key’ var to create a filter. Filters can be chained in order to 
/// create the desired effect, e.g. aspect fill, add a gradient, and add an overlay image. Since the image context is
/// shared, before changing any state to the ‘CGContext’ in the draw() method be sure to call ‘UIGraphicsPushContext’ and 
/// 'UIGraphicsPopContext’.
public protocol YapImageFilter {
  
  /// A unique key for this filter, used for caching to memory
  var key: String { get }
  
  /// The draw method to render into the bitmap context
  ///
  /// - parameter inContext:  The bitmap context for the filtered image
  /// - parameter image:      The original compressed 'UIImage' that is being filtered. Normally this is only used in the
  ///                         first pass in order to draw the source image, for example 'YapAspectFillFilter'.
  /// - parameter rect:       A 'CGRect' of the full bitmap.
  /// - parameter imageRect:  A 'CGrect' of the image to render, currently the same as imageRect, however will be used in
  ///                         the future to specify insets.
  func draw(inContext context: CGContext, image: UIImage?, rect: CGRect, imageRect: CGRect) -> Void
  
}

// MARK: Image filters

/// Draws the source 'image' with content mode aspect fill
public class YapAspectFillFilter: YapImageFilter {
  
  public var key = "AspectFill"
  
  public init() {}
  
  public func aspectFillRectForImage(withSize size: CGSize, inRect rect: CGRect) -> CGRect {
    
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
  
  public func draw(inContext context: CGContext, image: UIImage?, rect: CGRect, imageRect: CGRect) -> Void {
    guard let image = image, let cgImage = image.cgImage else { return }
    
    UIGraphicsPushContext(context)
    context.addRect(imageRect)
    context.clip()
    let aspectFillRect = aspectFillRectForImage(withSize: image.size, inRect: imageRect)
    context.draw(cgImage, in: aspectFillRect)
    UIGraphicsPopContext()
  }
}

/// Draws an overlay gradient from startColor to endColor
public class YapGradientFilter: YapImageFilter {
  
  var startColor: UIColor
  var endColor: UIColor
  
  private let numLocations: size_t = 2
  private let locations: [CGFloat] = [0.0, 1.0]
  private var components = [CGFloat]()
  
  public var key: String {
    
    var key = "Gradient"
    var r = CGFloat(0.0)
    var g = CGFloat(0.0)
    var b = CGFloat(0.0)
    var a = CGFloat(0.0)
    
    if endColor.getRed(&r, green: &g, blue: &b, alpha: &a) {
      key += String(format: "%0.5f%0.5f%0.5f%0.5f", r, g, b, a)
    }
    
    if startColor.getRed(&r, green: &g, blue: &b, alpha: &a) {
      key += String(format: "%0.5f%0.5f%0.5f%0.5f", r, g, b, a)
    }
    
    return key
  }
  
  /// Initializes the 'YapGradientFilter' with a given 'startColor' and 'endColor'
  ///
  /// - parameter startColor:  A 'UIColor' specifying the gradient start color.
  /// - parameter endColor:    A 'UIColor' specifying the gradient end color.
  ///
  /// - returns: The new 'YapGradientFilter'.
  public init(startColor: UIColor, endColor: UIColor) {
    self.startColor = startColor
    self.endColor = endColor
    
    var r = CGFloat(0.0)
    var g = CGFloat(0.0)
    var b = CGFloat(0.0)
    var a = CGFloat(0.0)
    
    if endColor.getRed(&r, green: &g, blue: &b, alpha: &a) {
      components.append(r)
      components.append(g)
      components.append(b)
      components.append(a)
    }
    
    if startColor.getRed(&r, green: &g, blue: &b, alpha: &a) {
      components.append(r)
      components.append(g)
      components.append(b)
      components.append(a)
    }
  }
  
  public func draw(inContext context: CGContext, image: UIImage?, rect: CGRect, imageRect: CGRect) -> Void {
    
    if let gradient = CGGradient(colorSpace: CGColorSpaceCreateDeviceRGB(), colorComponents: components, locations: locations, count: numLocations) {
      let top = CGPoint(x: CGFloat(0.0), y: CGFloat(0.0))
      let bottom = CGPoint(x: CGFloat(0.0), y: CGFloat(rect.size.height))
      context.drawLinearGradient(gradient, start: top, end: bottom, options: [])
    }
  }
}

/// Draws a background or overlay color
public class YapColorFilter: YapImageFilter {
  
  var color: UIColor
  
  public var key: String {
    
    var key = "Color"
    var r = CGFloat(0.0)
    var g = CGFloat(0.0)
    var b = CGFloat(0.0)
    var a = CGFloat(0.0)
    
    if color.getRed(&r, green: &g, blue: &b, alpha: &a) {
      key += String(format: "%0.5f%0.5f%0.5f%0.5f", r, g, b, a)
    }
    
    return key
  }
  
  /// Initializes the 'YapColorFilter' with a given 'color'
  ///
  /// - parameter color:  A 'UIColor' specifying the color.
  ///
  /// - returns: The new 'YapColorFilter'.
  public init(color: UIColor) {
    self.color = color
  }
  
  public func draw(inContext context: CGContext, image: UIImage?, rect: CGRect, imageRect: CGRect) -> Void {
    
    UIGraphicsPushContext(context)
    context.setFillColor(color.cgColor)
    context.fill(imageRect)
    UIGraphicsPopContext()
  }
}

/// Draws a background or overlay image
public class YapOverlayImageFilter: YapImageFilter {
  
  var overlayImage: UIImage

  /// The unique 'key' for the image, derived from the pointer to the 'overlayImage'. For optimal caching, use a static 
  /// ‘UIImage’ when initializing ‘YapOverlayImageFilter’, especially for use in recycled cells.
  public var key: String {
    
    var key = "Overlay"
    key += String(format: "%p", overlayImage)

    return key
  }
  
  /// Initializes the 'YapOverlayImageFilter' with a given 'color’. The unique 'key' for the image is derived from the 
  /// pointer to the specified ‘overlayImage'. For optimal caching, use a static ‘UIImage’ when initializing 
  /// ‘YapOverlayImageFilter’, especially for use in recycled cells.
  ///
  /// - parameter overlayImage:  A 'UIImage' specifying the overlay image.
  ///
  /// - returns: The new 'YapOverlayImageFilter'.
  public init(overlayImage: UIImage) {
    self.overlayImage = overlayImage
  }
  
  public func draw(inContext context: CGContext, image: UIImage?, rect: CGRect, imageRect: CGRect) -> Void {

    overlayImage.draw(in: imageRect)
  }
}

/// Adds a circle clip path, which will apply to all subsequent filters.
public class YapCircleClip: YapImageFilter {
	
	/// A unique key for this filter, used for caching to memory
	public var key = "Circle"
	
	public init() {}
	
	public func draw(inContext context: CGContext, image: UIImage?, rect: CGRect, imageRect: CGRect) -> Void {
		
		let fitDimension = min(imageRect.size.width, imageRect.size.height)
		let circleClip = CGSize(width: fitDimension, height: fitDimension)
		let radius = fitDimension / 2.0
		context.addEllipse(in: CGRect(origin: CGPoint(x: (imageRect.origin.x + imageRect.size.width / 2 - radius).rounded(), y: CGFloat(0.0)), size: circleClip).insetBy(dx: CGFloat(0.5), dy: CGFloat(0.5)))
		context.clip()
	}
}


