//
//  YapImageFilter.swift
//  YapImageManager
//
//  Created by Trevor Stout on 5/24/17.
//  Copyright (c) 2017 Yap Studios LLC. All rights reserved.
//

import Foundation

public protocol YapImageFilter {
  
  var key: String { get }
  
  func draw(inContext context: CGContext, image: UIImage?, rect: CGRect, imageRect: CGRect) -> Void
  
}

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

public class YapOverlayImageFilter: YapImageFilter {
  
  var overlayImage: UIImage
  
  public var key: String {
    
    var key = "Overlay"
    key += String(format: "%p", overlayImage)

    return key
  }
  
  public init(overlayImage: UIImage) {
    self.overlayImage = overlayImage
  }
  
  public func draw(inContext context: CGContext, image: UIImage?, rect: CGRect, imageRect: CGRect) -> Void {

    overlayImage.draw(in: imageRect)
  }
}


