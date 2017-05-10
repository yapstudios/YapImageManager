//
//  ImageCell.swift
//  YapImageManager
//
//  Created by Trevor Stout on 5/9/17.
//  Copyright © 2017 Yap Studios. All rights reserved.
//

import UIKit
import YapImageManager

class ImageCell: UICollectionViewCell {
  
  let imageView = UIImageView(frame: .zero)

  var URLString: String? {
    didSet {
      if URLString == nil {
        imageView.image = nil
      } else if oldValue != URLString {
        loadImage(withURLString: URLString!)
      }
    }
  }
  
  override init(frame: CGRect) {
    super.init(frame: frame)
    
    imageView.frame = self.bounds
    imageView.autoresizingMask = [.flexibleWidth, .flexibleHeight]
    imageView.contentMode = .scaleAspectFill
    addSubview(imageView)

    NotificationCenter.default.addObserver(self, selector: #selector(ImageCell.imageUpdated(_:)), name: YapImageManagerUpdatedNotification, object: nil)
  }
  
  deinit {
    NotificationCenter.default.removeObserver(self)
  }
  
  required init?(coder aDecoder: NSCoder) {
    fatalError("init(coder:) has not been implemented")
  }
  
  func loadImage(withURLString URLString: String) {
    YapImageManager.sharedInstance().image(forURLString: URLString, size: self.bounds.size) { (image: UIImage?, URLString: String) in
      if let image = image, URLString == self.URLString {
        self.imageView.image = image
      }
    }
  }
  
  func imageUpdated(_ notification: Notification) {
    
    if let URLString = notification.userInfo?[YapImageManagerURLKey] as? String {
      if URLString == self.URLString {
        loadImage(withURLString: URLString)
      }
    }
  }
  
  override func prepareForReuse() {
    URLString = nil
  }
}
