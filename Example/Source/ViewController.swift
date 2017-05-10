//
//  ViewController.swift
//  YapImageManager
//
//  Created by Trevor Stout on 5/3/17.
//  Copyright © 2017 Yap Studios. All rights reserved.
//

import UIKit
import Alamofire

let ImgurAPIClientId = "Client-ID fe5b53eb29bf6c4"

class ViewController: UIViewController {
  fileprivate var collectionView: UICollectionView?
  fileprivate let flowLayout = UICollectionViewFlowLayout()
  fileprivate let cellIdentifier = "cellIdentifier"
  
  fileprivate var images: [String]?

	func loadImages(forSubreddit subreddit: String, completion: @escaping ([String]?) -> Void) {
		var headers = [String: String]()
		headers["Authorization"] = ImgurAPIClientId
		let url = "https://api.imgur.com/3/gallery/r/\(subreddit)/time/0.json"
		
		SessionManager.default.request(url, method: .get, parameters: nil, encoding: JSONEncoding.default, headers: headers)
			.validate()
			.responseJSON() { response in
				
				switch response.result {
				case .success:
					if let result = response.result.value as? [String: AnyObject], let data = result["data"] as? [[String: AnyObject]] {
						let images = data.flatMap { $0["link"] as? String }
						completion(images)
					} else {
						completion(nil)
					}
				case .failure (let error):
					print("Error loading images: \(error)")
					completion(nil)
				}
		}
	}

	override func viewDidLoad() {
		super.viewDidLoad()

    let inset = CGFloat(10.0)
    flowLayout.minimumLineSpacing = inset
    let imageWidth = view.bounds.width - 2.0 * inset
    flowLayout.itemSize = CGSize(width: imageWidth, height: (9.0 / 16.0 * imageWidth).rounded())
    collectionView = UICollectionView(frame: self.view.bounds, collectionViewLayout: flowLayout)
    collectionView!.contentInset = UIEdgeInsetsMake(80.0, 0.0, inset, 0.0)
    collectionView!.autoresizingMask = [.flexibleWidth, .flexibleHeight]
    collectionView!.dataSource = self
    collectionView!.backgroundColor = .clear
    collectionView!.register(ImageCell.self, forCellWithReuseIdentifier: cellIdentifier)
    collectionView!.alwaysBounceVertical = true
    view.addSubview(self.collectionView!)
    
		loadImages(forSubreddit: "earthporn") { images in
			if let images = images {
        self.images = images
        self.collectionView?.reloadData()
			}
		}
	}  
}

extension ViewController: UICollectionViewDataSource {
  
  public func collectionView(_ collectionView: UICollectionView, numberOfItemsInSection section: Int) -> Int {
    return images?.count ?? 0
  }

  public func collectionView(_ collectionView: UICollectionView, cellForItemAt indexPath: IndexPath) -> UICollectionViewCell {
    let cell = collectionView.dequeueReusableCell(withReuseIdentifier: cellIdentifier, for: indexPath) as! ImageCell
    cell.backgroundColor = .lightGray
    cell.URLString = images![indexPath.item]
    return cell
  }
}
