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
  
  // filters
  static let overlayImage = UIImage(named: "tileShadow")!.resizableImage(withCapInsets: UIEdgeInsetsMake(4.0, 4.0, 4.0, 4.0))
  let overlay = YapOverlayImageFilter(overlayImage: ImageCell.overlayImage)
  let aspectFill = YapAspectFillFilter()

  var URLString: String? {
    didSet {
			if oldValue != URLString, let ticket = self.ticket {
				YapImageManager.sharedInstance.cancelImageRequest(forTicket: ticket)
				self.ticket = nil
			}
      if URLString == nil {
        imageView.image = nil
      } else if oldValue != URLString {
        loadImage(withURLString: URLString!)
      }
    }
  }
	
	private var ticket: ImageRequestTicket?
	
  override init(frame: CGRect) {
    super.init(frame: frame)
    
    imageView.frame = self.bounds
    imageView.autoresizingMask = [.flexibleWidth, .flexibleHeight]
    imageView.contentMode = .topLeft
    addSubview(imageView)
  }
  
  required init?(coder aDecoder: NSCoder) {
    fatalError("init(coder:) has not been implemented")
  }
  
  func loadImage(withURLString URLString: String) {

    let filters: [YapImageFilter] = [aspectFill, overlay]
		ticket = YapImageManager.sharedInstance.asyncImage(forURLString: URLString, size: self.bounds.size, filters: filters) { [weak self] response in
			
			if response.ticket == self?.ticket {
				self?.ticket = nil
			
				if let image = response.image {
					self?.imageView.image = image
				}
			}
    }
  }
  
  override func prepareForReuse() {
    URLString = nil
  }
}
